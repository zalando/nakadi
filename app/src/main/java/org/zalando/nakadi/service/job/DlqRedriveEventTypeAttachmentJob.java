package org.zalando.nakadi.service.job;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.zalando.nakadi.cache.EventTypeCache;
import org.zalando.nakadi.config.NakadiSettings;
import org.zalando.nakadi.domain.ConsumedEvent;
import org.zalando.nakadi.domain.EventTypePartition;
import org.zalando.nakadi.domain.Feature;
import org.zalando.nakadi.domain.HeaderTag;
import org.zalando.nakadi.domain.NakadiCursor;
import org.zalando.nakadi.domain.Subscription;
import org.zalando.nakadi.repository.db.SubscriptionDbRepository;
import org.zalando.nakadi.service.CursorConverter;
import org.zalando.nakadi.service.FeatureToggleService;
import org.zalando.nakadi.service.subscription.model.Partition;
import org.zalando.nakadi.service.timeline.HighLevelConsumer;
import org.zalando.nakadi.service.timeline.TimelineService;
import org.zalando.nakadi.view.Cursor;
import org.zalando.nakadi.service.subscription.zk.SubscriptionClientFactory;
import org.zalando.nakadi.service.subscription.zk.ZkSubscriptionClient;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

@Service
public class DlqRedriveEventTypeAttachmentJob {

    private static final String JOB_NAME = "dlq-redrive-event-type-attachment";

    private static final Logger LOG = LoggerFactory.getLogger(DlqRedriveEventTypeAttachmentJob.class);

    private final FeatureToggleService featureToggleService;
    private final EventTypeCache eventTypeCache;
    private final TimelineService timelineService;
    private final SubscriptionDbRepository subscriptionRepository;
    private final SubscriptionClientFactory subscriptionClientFactory;
    private final CursorConverter cursorConverter;
    private final NakadiSettings nakadiSettings;
    private final String dlqRedriveEventTypeName;
    private final ExclusiveJobWrapper jobWrapper;

    @Autowired
    public DlqRedriveEventTypeAttachmentJob(
            final FeatureToggleService featureToggleService,
            final EventTypeCache eventTypeCache,
            final TimelineService timelineService,
            final SubscriptionDbRepository subscriptionRepository,
            final SubscriptionClientFactory subscriptionClientFactory,
            final CursorConverter cursorConverter,
            final NakadiSettings nakadiSettings,
            @Value("${nakadi.dlq.redriveEventTypeName}") final String dlqRedriveEventTypeName,
            @Value("${nakadi.jobs.dlqRedriveEventTypeAttach.runPeriodMs}") final int periodMs,
            final JobWrapperFactory jobWrapperFactory) {
        this.featureToggleService = featureToggleService;
        this.eventTypeCache = eventTypeCache;
        this.timelineService = timelineService;
        this.subscriptionRepository = subscriptionRepository;
        this.subscriptionClientFactory = subscriptionClientFactory;
        this.cursorConverter = cursorConverter;
        this.nakadiSettings = nakadiSettings;
        this.dlqRedriveEventTypeName = dlqRedriveEventTypeName;
        this.jobWrapper = jobWrapperFactory.createExclusiveJobWrapper(JOB_NAME, periodMs);
    }

    @Scheduled(
            fixedDelayString = "${nakadi.jobs.checkRunMs}",
            initialDelayString = "${random.int(${nakadi.jobs.checkRunMs})}")
    public void attachDlqRedriveEventType() {
        if (featureToggleService.isFeatureEnabled(Feature.DLQ_REDRIVE_EVENT_TYPE_ATTACHMENT_JOB)) {
            LOG.warn("Feature toggle is disabled for DLQ redrive event type attachment job: skipping the run.");
            return;
        }
        try {
            jobWrapper.runJobLocked(() -> {
                    try {
                        attachDlqRedriveEventTypeLocked();
                    } catch (final IOException ioe) {
                        throw new RuntimeException(ioe);
                    }
                });
        } catch (final Exception e) {
            LOG.error("Failed to run the job to attach DLQ redrive event type to corresponding subscriptions", e);
        }
    }

    private void attachDlqRedriveEventTypeLocked() throws IOException {

        final Set<String> subscriptionIds = new HashSet<>();

        final List<String> orderedPartitions = eventTypeCache.getOrderedPartitions(dlqRedriveEventTypeName);

        // it's inefficient to re-consume from begin, but we expect this event type to be empty most of the time anyway
        final List<NakadiCursor> beginCursors = orderedPartitions.stream()
                .map(p -> new Cursor(p, Cursor.BEFORE_OLDEST_OFFSET))
                .map(c -> cursorConverter.convert(dlqRedriveEventTypeName, c))
                .collect(Collectors.toList());

        try (HighLevelConsumer consumer = timelineService.createEventConsumer(JOB_NAME + "-job", beginCursors)) {
            while (true) {
                final List<ConsumedEvent> events = consumer.readEvents();
                if (events.isEmpty()) {
                    // consumed till the current end
                    break;
                }
                events.forEach(e -> {
                        final Map<HeaderTag, String> tags = e.getConsumerTags();
                        if (tags != null) {
                            final String sid = tags.get(HeaderTag.CONSUMER_SUBSCRIPTION_ID);
                            if (sid != null) {
                                subscriptionIds.add(sid);
                            }
                        }
                    });
            }
        }

        final List<Partition> unassignedDlqPartitions = orderedPartitions.stream()
                .map(p -> new Partition(dlqRedriveEventTypeName, p, null, null, Partition.State.UNASSIGNED))
                .collect(Collectors.toList());

        // for every subscription update the topology, if needed, so that the dlq redrive event type is included
        subscriptionIds.forEach(sid -> {
                    try {
                        addDlqPartitionsToSubscription(
                                subscriptionRepository.getSubscription(sid),
                                unassignedDlqPartitions);
                    } catch (final Exception e) {
                        // subscription is already gone, but events for redrive are still there; or failed to update
                        // topology, etc.
                        LOG.warn("Failed to add DLQ redrive event type to subscription: {}", sid, e);
                    }
                });
    }

    private void addDlqPartitionsToSubscription(
            final Subscription subscription,
            final List<Partition> unassignedDlqPartitions) throws Exception {

        try (ZkSubscriptionClient client = subscriptionClientFactory.createClient(subscription)) {

            // idempotent, does not overwrite existing offsets
            client.createOffsetZNodes(
                    unassignedDlqPartitions.stream()
                            .map(p -> p.getPartition())
                            .map(p -> new Cursor(p, Cursor.BEFORE_OLDEST_OFFSET))
                            .map(c -> cursorConverter.convert(dlqRedriveEventTypeName, c))
                            .map(nc -> cursorConverter.convertToNoToken(nc))
                            .collect(Collectors.toList()));

            final boolean[] hasNewPartitions = {false};
            client.updateTopology(topology -> {
                final var newPartitions = selectMissingPartitions(topology.getPartitions(), unassignedDlqPartitions);
                if (!hasNewPartitions[0] && newPartitions.length != 0) {
                    hasNewPartitions[0] = true;
                }
                return newPartitions;
            });

            if (hasNewPartitions[0]) {
                LOG.info("Rebalancing `{}` subscription's sessions due to addition of " +
                        "Nakadi DLQ Event Type", subscription.getId());
                client.rebalanceSessions();
            }
        }
    }

    static Partition[] selectMissingPartitions(
            final Partition[] initialPartitions,
            final List<Partition> toAddPartitions) {

        final Map<EventTypePartition, Partition> assignment =
                Arrays.stream(initialPartitions).collect(Collectors.toMap(k -> k.getKey(), v -> v));

        return toAddPartitions.stream().filter(p -> !assignment.containsKey(p.getKey()))
                .toArray(Partition[]::new);
    }
}
