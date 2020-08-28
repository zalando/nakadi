package org.zalando.nakadi.service;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.zalando.nakadi.cache.EventTypeCache;
import org.zalando.nakadi.config.NakadiSettings;
import org.zalando.nakadi.domain.EventType;
import org.zalando.nakadi.domain.EventTypeStatistics;
import org.zalando.nakadi.domain.NakadiCursor;
import org.zalando.nakadi.domain.Subscription;
import org.zalando.nakadi.domain.SubscriptionBase;
import org.zalando.nakadi.exceptions.runtime.EventTypeDeletionException;
import org.zalando.nakadi.exceptions.runtime.EventTypeUnavailableException;
import org.zalando.nakadi.exceptions.runtime.InternalNakadiException;
import org.zalando.nakadi.exceptions.runtime.InvalidEventTypeException;
import org.zalando.nakadi.exceptions.runtime.NakadiBaseException;
import org.zalando.nakadi.exceptions.runtime.NakadiRuntimeException;
import org.zalando.nakadi.exceptions.runtime.ServiceTemporarilyUnavailableException;
import org.zalando.nakadi.repository.db.EventTypeRepository;
import org.zalando.nakadi.repository.db.SubscriptionDbRepository;
import org.zalando.nakadi.service.subscription.LogPathBuilder;
import org.zalando.nakadi.service.subscription.zk.SubscriptionClientFactory;
import org.zalando.nakadi.service.subscription.zk.ZkSubscriptionClient;
import org.zalando.nakadi.service.timeline.TimelineService;
import org.zalando.nakadi.service.timeline.TimelineSync;
import org.zalando.nakadi.view.Cursor;
import org.zalando.nakadi.view.SubscriptionCursorWithoutToken;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

@Service
public class RepartitioningService {

    private static final Logger LOG = LoggerFactory.getLogger(RepartitioningService.class);

    private final EventTypeRepository eventTypeRepository;
    private final TimelineService timelineService;
    private final SubscriptionDbRepository subscriptionRepository;
    private final SubscriptionClientFactory subscriptionClientFactory;
    private final Integer repartitioningSubscriptionsLimit;
    private final NakadiSettings nakadiSettings;
    private final CursorConverter cursorConverter;
    private final FeatureToggleService featureToggleService;
    private final EventTypeCache eventTypeCache;
    private final TimelineSync timelineSync;

    @Autowired
    public RepartitioningService(
            final EventTypeRepository eventTypeRepository,
            final TimelineService timelineService,
            final SubscriptionDbRepository subscriptionRepository,
            final SubscriptionClientFactory subscriptionClientFactory,
            @Value("${nakadi.repartitioning.subscriptions.limit:1000}") final Integer repartitioningSubscriptionsLimit,
            final NakadiSettings nakadiSettings,
            final CursorConverter cursorConverter,
            final FeatureToggleService featureToggleService,
            final EventTypeCache eventTypeCache,
            final TimelineSync timelineSync) {
        this.eventTypeRepository = eventTypeRepository;
        this.timelineService = timelineService;
        this.subscriptionRepository = subscriptionRepository;
        this.subscriptionClientFactory = subscriptionClientFactory;
        this.repartitioningSubscriptionsLimit = repartitioningSubscriptionsLimit;
        this.nakadiSettings = nakadiSettings;
        this.cursorConverter = cursorConverter;
        this.featureToggleService = featureToggleService;
        this.eventTypeCache = eventTypeCache;
        this.timelineSync = timelineSync;
    }

    public void repartition(final EventType eventType, final int partitions)
            throws InternalNakadiException, NakadiRuntimeException {
        if (partitions > nakadiSettings.getMaxTopicPartitionCount()) {
            throw new InvalidEventTypeException("Number of partitions should not be more than "
                    + nakadiSettings.getMaxTopicPartitionCount());
        }

        EventTypeStatistics defaultStatistic = eventType.getDefaultStatistic();
        if (defaultStatistic == null) {
            defaultStatistic = new EventTypeStatistics(1, 1);
            eventType.setDefaultStatistic(defaultStatistic);
        }
        final int currentPartitionsNumber = Math.max(defaultStatistic.getReadParallelism(),
                defaultStatistic.getWriteParallelism());
        if (partitions < currentPartitionsNumber) {
            throw new InvalidEventTypeException("Number of partitions should be greater " +
                    "than existing values.");
        }

        LOG.info("Start repartitioning for {} to {} partitions", eventType.getName(), partitions);

        Closeable closeable = null;
        try {
            closeable = timelineSync.workWithEventType(eventType.getName(), nakadiSettings.getTimelineWaitTimeoutMs());
            timelineService.updateTimeLineForRepartition(eventType, partitions);

            updateSubscriptionsForRepartitioning(eventType, partitions);

            // it is clear that the operation has to be done under the lock with other related work for changing event
            // type, but it is skipped, because it is quite rare operation to change event type and repartition at the
            // same time
            try {
                defaultStatistic.setReadParallelism(partitions);
                defaultStatistic.setWriteParallelism(partitions);
                eventTypeRepository.update(eventType);
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

    private void updateSubscriptionsForRepartitioning(final EventType eventType, final int partitions)
            throws NakadiBaseException {
        final List<Subscription> subscriptions = subscriptionRepository.listSubscriptions(
                ImmutableSet.of(eventType.getName()), Optional.empty(), 0, repartitioningSubscriptionsLimit);
        for (final Subscription subscription : subscriptions) {
            // update subscription if it was created from cursors
            if (subscription.getReadFrom() == SubscriptionBase.InitialPosition.CURSORS) {
                final List<SubscriptionCursorWithoutToken> initialCursors = subscription.getInitialCursors().stream()
                        .filter(cursor -> cursor.getEventType().equals(eventType.getName()))
                        .collect(Collectors.toList());
                final List<SubscriptionCursorWithoutToken> newInitialCursors = Lists.newArrayList(initialCursors);
                while (partitions - newInitialCursors.size() > 0) {
                    newInitialCursors.add(new SubscriptionCursorWithoutToken(
                            eventType.getName(),
                            String.valueOf(newInitialCursors.size()),
                            Cursor.BEFORE_OLDEST_OFFSET));
                }
                subscription.setInitialCursors(newInitialCursors);
                subscriptionRepository.updateSubscription(subscription);
            }

            final ZkSubscriptionClient zkClient = subscriptionClientFactory.createClient(subscription,
                    LogPathBuilder.build(subscription.getId(), "repartition"));
            // it could be that subscription was created, but never initialized
            SubscriptionInitializer.initializeSubscriptionLocked(
                    zkClient, subscription, timelineService, cursorConverter);
            // get begin offset with timeline, partition does not matter, it will be the same for all partitions
            final Cursor cursor = cursorConverter.convert(
                    NakadiCursor.of(timelineService.getActiveTimeline(eventType.getName()), null, "-1"));
            zkClient.repartitionTopology(eventType.getName(), partitions, cursor.getOffset());
            zkClient.closeSubscriptionStreams(
                    () -> LOG.info("subscription streams were closed, after repartitioning"),
                    TimeUnit.SECONDS.toMillis(nakadiSettings.getMaxCommitTimeout()));
        }
    }
}
