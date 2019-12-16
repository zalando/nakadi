package org.zalando.nakadi.service;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.transaction.support.TransactionTemplate;
import org.zalando.nakadi.config.NakadiSettings;
import org.zalando.nakadi.domain.EventType;
import org.zalando.nakadi.domain.EventTypeStatistics;
import org.zalando.nakadi.domain.NakadiCursor;
import org.zalando.nakadi.domain.Subscription;
import org.zalando.nakadi.domain.SubscriptionBase;
import org.zalando.nakadi.exceptions.runtime.InternalNakadiException;
import org.zalando.nakadi.exceptions.runtime.InvalidEventTypeException;
import org.zalando.nakadi.exceptions.runtime.NakadiBaseException;
import org.zalando.nakadi.exceptions.runtime.NakadiRuntimeException;
import org.zalando.nakadi.repository.EventTypeRepository;
import org.zalando.nakadi.repository.db.SubscriptionDbRepository;
import org.zalando.nakadi.service.subscription.LogPathBuilder;
import org.zalando.nakadi.service.subscription.state.StartingState;
import org.zalando.nakadi.service.subscription.zk.SubscriptionClientFactory;
import org.zalando.nakadi.service.subscription.zk.ZkSubscriptionClient;
import org.zalando.nakadi.service.timeline.TimelineService;
import org.zalando.nakadi.view.Cursor;
import org.zalando.nakadi.view.SubscriptionCursorWithoutToken;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

@Service
public class RepartitioningService {

    private static final Logger LOG = LoggerFactory.getLogger(RepartitioningService.class);

    private final TransactionTemplate transactionTemplate;
    private final EventTypeRepository eventTypeRepository;
    private final TimelineService timelineService;
    private final SubscriptionDbRepository subscriptionRepository;
    private final SubscriptionClientFactory subscriptionClientFactory;
    private final Integer repartitioningSubscriptionsLimit;
    private final NakadiSettings nakadiSettings;
    private final CursorConverter cursorConverter;
    private final FeatureToggleService featureToggleService;

    @Autowired
    public RepartitioningService(
            final TransactionTemplate transactionTemplate,
            final EventTypeRepository eventTypeRepository,
            final TimelineService timelineService,
            final SubscriptionDbRepository subscriptionRepository,
            final SubscriptionClientFactory subscriptionClientFactory,
            @Value("${nakadi.repartitioning.subscriptions.limit:1000}") final Integer repartitioningSubscriptionsLimit,
            final NakadiSettings nakadiSettings,
            final CursorConverter cursorConverter,
            final FeatureToggleService featureToggleService) {
        this.transactionTemplate = transactionTemplate;
        this.eventTypeRepository = eventTypeRepository;
        this.timelineService = timelineService;
        this.subscriptionRepository = subscriptionRepository;
        this.subscriptionClientFactory = subscriptionClientFactory;
        this.repartitioningSubscriptionsLimit = repartitioningSubscriptionsLimit;
        this.nakadiSettings = nakadiSettings;
        this.cursorConverter = cursorConverter;
        this.featureToggleService = featureToggleService;
    }

    public void repartition(final EventType eventType, final int partitions)
            throws InternalNakadiException, NakadiRuntimeException {
        LOG.info("start repartitioning for {} to {} partitions", eventType.getName(), partitions);
        transactionTemplate.execute(action -> {
            eventTypeRepository.update(eventType);
            timelineService.updateTimeLineForRepartition(eventType, partitions);
            return null;
        });

        eventTypeRepository.notifyUpdated(eventType.getName());
        updateSubscriptionsForRepartitioning(eventType, partitions);
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
            StartingState.initializeSubscriptionLocked(zkClient, subscription, timelineService, cursorConverter);
            // get begin offset with timeline, partition does not matter, it will be the same for all partitions
            final Cursor cursor = cursorConverter.convert(
                    NakadiCursor.of(timelineService.getActiveTimeline(eventType.getName()), null, "-1"));
            zkClient.repartitionTopology(eventType.getName(), partitions, cursor.getOffset());
            zkClient.closeSubscriptionStreams(
                    () -> LOG.info("subscription streams were closed, after repartitioning"),
                    TimeUnit.SECONDS.toMillis(nakadiSettings.getMaxCommitTimeout()));
        }
    }

    public void checkAndRepartition(final EventType original,
                                    final EventType newEventType)
            throws InvalidEventTypeException {
        if (!featureToggleService.isFeatureEnabled(FeatureToggleService.Feature.REPARTITIONING)) {
            return;
        }

        final EventTypeStatistics existingStatistics = original.getDefaultStatistic();
        final EventTypeStatistics newStatistics = newEventType.getDefaultStatistic();
        if (newStatistics == null) {
            return;
        }

        final int oldMaxPartitions;
        if (existingStatistics == null) {
            oldMaxPartitions = 1;
        } else {
            oldMaxPartitions = Math.max(existingStatistics.getReadParallelism(),
                    existingStatistics.getWriteParallelism());
        }

        final int newMaxPartitions = Math.max(newStatistics.getReadParallelism(),
                newStatistics.getWriteParallelism());
        if (newMaxPartitions > nakadiSettings.getMaxTopicPartitionCount()) {
            throw new InvalidEventTypeException("Number of partitions should not be more than "
                    + nakadiSettings.getMaxTopicPartitionCount());
        }
        if (newMaxPartitions < oldMaxPartitions) {
            throw new InvalidEventTypeException("Read and write parallelism should be greater " +
                    "than existing values.");
        }
        if (newMaxPartitions == oldMaxPartitions) {
            // avoid shuffling
            if ((existingStatistics.getReadParallelism() != newStatistics.getReadParallelism()) ||
                    (existingStatistics.getWriteParallelism() != newStatistics.getWriteParallelism())) {
                throw new InvalidEventTypeException("Read and write parallelism can be changed only to change" +
                        "the number of partition (max of read and write parallelism)");
            }
            return;
        }

        repartition(newEventType, newMaxPartitions);
    }

}
