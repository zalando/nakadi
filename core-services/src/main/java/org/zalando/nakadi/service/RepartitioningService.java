package org.zalando.nakadi.service;

import com.google.common.collect.ImmutableSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.zalando.nakadi.cache.EventTypeCache;
import org.zalando.nakadi.config.NakadiSettings;
import org.zalando.nakadi.domain.EventType;
import org.zalando.nakadi.domain.EventTypeStatistics;
import org.zalando.nakadi.domain.NakadiCursor;
import org.zalando.nakadi.domain.Subscription;
import org.zalando.nakadi.domain.SubscriptionBase;
import org.zalando.nakadi.exceptions.runtime.EventTypeUnavailableException;
import org.zalando.nakadi.exceptions.runtime.InternalNakadiException;
import org.zalando.nakadi.exceptions.runtime.InvalidEventTypeException;
import org.zalando.nakadi.exceptions.runtime.NakadiBaseException;
import org.zalando.nakadi.exceptions.runtime.NakadiRuntimeException;
import org.zalando.nakadi.repository.db.EventTypeRepository;
import org.zalando.nakadi.repository.db.SubscriptionDbRepository;
import org.zalando.nakadi.service.subscription.zk.SubscriptionClientFactory;
import org.zalando.nakadi.service.subscription.zk.ZkSubscriptionClient;
import org.zalando.nakadi.service.timeline.TimelineService;
import org.zalando.nakadi.service.timeline.TimelineSync;
import org.zalando.nakadi.util.MDCUtils;
import org.zalando.nakadi.view.Cursor;
import org.zalando.nakadi.view.SubscriptionCursorWithoutToken;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

@Service
public class RepartitioningService {

    private static final Logger LOG = LoggerFactory.getLogger(RepartitioningService.class);

    private final EventTypeRepository eventTypeRepository;
    private final EventTypeCache eventTypeCache;
    private final TimelineService timelineService;
    private final SubscriptionDbRepository subscriptionRepository;
    private final SubscriptionClientFactory subscriptionClientFactory;
    private final NakadiSettings nakadiSettings;
    private final CursorConverter cursorConverter;
    private final TimelineSync timelineSync;

    @Autowired
    public RepartitioningService(
            final EventTypeRepository eventTypeRepository,
            final EventTypeCache eventTypeCache,
            final TimelineService timelineService,
            final SubscriptionDbRepository subscriptionRepository,
            final SubscriptionClientFactory subscriptionClientFactory,
            final NakadiSettings nakadiSettings,
            final CursorConverter cursorConverter,
            final TimelineSync timelineSync) {
        this.eventTypeRepository = eventTypeRepository;
        this.eventTypeCache = eventTypeCache;
        this.timelineService = timelineService;
        this.subscriptionRepository = subscriptionRepository;
        this.subscriptionClientFactory = subscriptionClientFactory;
        this.nakadiSettings = nakadiSettings;
        this.cursorConverter = cursorConverter;
        this.timelineSync = timelineSync;
    }

    public void repartition(final String eventTypeName, final int partitions)
            throws InternalNakadiException, NakadiRuntimeException {
        if (partitions > nakadiSettings.getMaxTopicPartitionCount()) {
            throw new InvalidEventTypeException("Number of partitions should not be more than "
                    + nakadiSettings.getMaxTopicPartitionCount());
        }
        LOG.info("Start repartitioning for {} to {} partitions", eventTypeName, partitions);
        final EventType eventType = eventTypeRepository.findByName(eventTypeName);

        eventType.setDefaultStatistic(Optional.ofNullable(eventType.getDefaultStatistic())
                .orElse(new EventTypeStatistics(1, 1)));
        final int currentPartitionsNumber =
                timelineService.getTopicRepository(eventType).listPartitionNames(
                        timelineService.getActiveTimeline(eventType).getTopic()).size();
        if (partitions < currentPartitionsNumber) {
            // It is fine if user asks to set partition count to the same value several times,
            // as it may happen that the first request was not really successful
            throw new InvalidEventTypeException("Number of partitions can not decrease");
        }
        Closeable closeable = null;
        try {
            closeable = timelineSync.workWithEventType(eventType.getName(), nakadiSettings.getTimelineWaitTimeoutMs());
            // Increase kafka partitions count, increase partitions in database
            timelineService.updateTimeLineForRepartition(eventType, partitions);

            subscriptionRepository.listAllSubscriptionsFor(ImmutableSet.of(eventType.getName()))
                    .forEach(sub -> {
                        try (MDCUtils.CloseableNoEx ignore = MDCUtils.withSubscriptionId(sub.getId())) {
                            updateSubscriptionForRepartitioning(sub, eventTypeName, partitions);
                        }
                    });

            // it is clear that the operation has to be done under the lock with other related work for changing event
            // type, but it is skipped, because it is quite rare operation to change event type and repartition at the
            // same time
            try {
                eventType.getDefaultStatistic().setReadParallelism(partitions);
                eventType.getDefaultStatistic().setWriteParallelism(partitions);
                eventTypeRepository.update(eventType);
                eventTypeCache.invalidate(eventTypeName);
            } catch (Exception e) {
                throw new NakadiBaseException(e.getMessage(), e);
            }
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
            LOG.error("Failed to wait for timeline switch", e);
            throw new EventTypeUnavailableException("Event type " + eventType.getName()
                    + " is currently in maintenance, please repeat request");
        } catch (final TimeoutException e) {
            LOG.error("Failed to wait for timeline switch", e);
            throw new EventTypeUnavailableException("Event type " + eventType.getName()
                    + " is currently in maintenance, please repeat request");
        } finally {
            try {
                if (closeable != null) {
                    closeable.close();
                }
            } catch (final IOException e) {
                LOG.error("Exception occurred when releasing usage of event-type", e);
            }
        }
    }

    private void updateSubscriptionForRepartitioning(
            final Subscription subscription, final String eventTypeName, final int partitions)
            throws NakadiBaseException {
        // update subscription if it was created from cursors
        if (subscription.getReadFrom() == SubscriptionBase.InitialPosition.CURSORS) {
            // 1. Create a copy of initial cursors
            final List<SubscriptionCursorWithoutToken> newInitialCursors =
                    new ArrayList<>(subscription.getInitialCursors());
            // 2. Select the ones that are related to event type that is being modified
            final int currentCursorCount = (int) newInitialCursors.stream()
                    .filter(v -> v.getEventType().equals(eventTypeName))
                    .count();
            // 3. Add as many cursors as needed.
            for (int partitionIdx = partitions; partitionIdx < currentCursorCount; ++partitionIdx) {
                newInitialCursors.add(new SubscriptionCursorWithoutToken(
                        eventTypeName,
                        String.valueOf(partitionIdx),
                        Cursor.BEFORE_OLDEST_OFFSET));
            }
            subscription.setInitialCursors(newInitialCursors);
            subscriptionRepository.updateSubscription(subscription);
        }

        final ZkSubscriptionClient zkClient = subscriptionClientFactory.createClient(subscription);
        try {
            // it could be that subscription was created, but never initialized
            SubscriptionInitializer.initialize(
                    zkClient, subscription, timelineService, cursorConverter);
            // get begin offset with timeline, partition does not matter, it will be the same for all partitions
            final Cursor cursor = cursorConverter.convert(
                    NakadiCursor.of(timelineService.getActiveTimeline(eventTypeName), null, "-1"));
            zkClient.repartitionTopology(eventTypeName, partitions, cursor.getOffset());
            zkClient.closeSubscriptionStreams(
                    () -> LOG.info("subscription streams were closed, after repartitioning"),
                    TimeUnit.SECONDS.toMillis(nakadiSettings.getMaxCommitTimeout()));
        } finally {
            try {
                zkClient.close();
            } catch (final IOException ex) {
                LOG.warn("Failed to close zookeeper connection while updating subsciprtion {}",
                        subscription.getId(), ex);
            }
        }
    }
}
