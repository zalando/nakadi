package org.zalando.nakadi.service.job;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.zalando.nakadi.cache.EventTypeCache;
import org.zalando.nakadi.domain.ConsumedEvent;
import org.zalando.nakadi.domain.EventTypePartition;
import org.zalando.nakadi.domain.Feature;
import org.zalando.nakadi.domain.HeaderTag;
import org.zalando.nakadi.domain.NakadiCursor;
import org.zalando.nakadi.domain.Subscription;
import org.zalando.nakadi.service.CursorConverter;
import org.zalando.nakadi.service.FeatureToggleService;
import org.zalando.nakadi.service.SubscriptionService;
import org.zalando.nakadi.service.subscription.model.Partition;
import org.zalando.nakadi.service.timeline.HighLevelConsumer;
import org.zalando.nakadi.service.timeline.TimelineService;
import org.zalando.nakadi.view.Cursor;
import org.zalando.nakadi.service.subscription.zk.SubscriptionClientFactory;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Service
public class DlqRedriveEventTypeAttachmentJob {

    private static final String JOB_NAME = "dlq-redrive-event-type-attachment";

    private static final Logger LOG = LoggerFactory.getLogger(DlqRedriveEventTypeAttachmentJob.class);

    private final FeatureToggleService featureToggleService;
    private final EventTypeCache eventTypeCache;
    private final TimelineService timelineService;
    private final SubscriptionService subscriptionService;
    private final SubscriptionClientFactory subscriptionClientFactory;
    private final CursorConverter cursorConverter;
    private final String dlqRedriveEventTypeName;
    private final ExclusiveJobWrapper jobWrapper;

    @Autowired
    public DlqRedriveEventTypeAttachmentJob(
            final FeatureToggleService featureToggleService,
            final EventTypeCache eventTypeCache,
            final TimelineService timelineService,
            final SubscriptionService subscriptionService,
            final SubscriptionClientFactory subscriptionClientFactory,
            final CursorConverter cursorConverter,
            @Value("${nakadi.dlq.redriveEventTypeName}") final String dlqRedriveEventTypeName,
            @Value("${nakadi.jobs.dlqRedriveEventTypeAttach.runPeriodMs}") final int periodMs,
            final JobWrapperFactory jobWrapperFactory) {
        this.featureToggleService = featureToggleService;
        this.eventTypeCache = eventTypeCache;
        this.timelineService = timelineService;
        this.subscriptionService = subscriptionService;
        this.subscriptionClientFactory = subscriptionClientFactory;
        this.cursorConverter = cursorConverter;
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
            jobWrapper.runJobLocked(this::attachDlqRedriveEventTypeLocked);
        } catch (final Exception e) {
            LOG.error("Failed to run the job to attach DLQ redrive event type to corresponding subscriptions", e);
        }
    }

    private void attachDlqRedriveEventTypeLocked() {

        final List<String> orderedPartitions = eventTypeCache.getOrderedPartitions(dlqRedriveEventTypeName);

        final List<NakadiCursor> beginCursors = orderedPartitions.stream()
                .map(p -> new Cursor(p, Cursor.BEFORE_OLDEST_OFFSET))
                .map(c -> cursorConverter.convert(dlqRedriveEventTypeName, c))
                .collect(Collectors.toList());

        final HighLevelConsumer consumer = timelineService.createEventConsumer(JOB_NAME + "-job", beginCursors);

        final Set<String> subscriptionIds = new HashSet<>();
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

        final List<Partition> unassignedDlqPartitions = orderedPartitions.stream()
                .map(p -> new Partition(dlqRedriveEventTypeName, p, null, null, Partition.State.UNASSIGNED))
                .collect(Collectors.toList());

        // for every subscription update the topology, if needed, so that the dlq redrive event type is included
        subscriptionIds.forEach(sid ->
                updateSubscriptionTopology(
                        subscriptionService.getSubscription(sid),
                        unassignedDlqPartitions));
    }

    private void updateSubscriptionTopology(
            final Subscription subscription,
            final List<Partition> unassignedDlqPartitions) {

        subscriptionClientFactory
                .createClient(subscription)
                .updateTopology(topology -> addPartitionsIfMissing(topology.getPartitions(), unassignedDlqPartitions));
    }

    private static Partition[] addPartitionsIfMissing(
            final Partition[] initialPartitions,
            final List<Partition> toAddPartitions) {

        final Map<EventTypePartition, Partition> assignment =
                Arrays.stream(initialPartitions).collect(Collectors.toMap(k -> k.getKey(), v -> v));
        return
                Stream.concat(assignment.entrySet().stream(),
                        toAddPartitions.stream().filter(p -> !assignment.containsKey(p.getKey())))
                .toArray(Partition[]::new);
    }
}
