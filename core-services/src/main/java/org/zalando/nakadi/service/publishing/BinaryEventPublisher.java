package org.zalando.nakadi.service.publishing;

import io.opentracing.Span;
import io.opentracing.tag.Tags;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;
import org.zalando.nakadi.config.NakadiSettings;
import org.zalando.nakadi.domain.EventType;
import org.zalando.nakadi.domain.NakadiRecord;
import org.zalando.nakadi.domain.NakadiRecordResult;
import org.zalando.nakadi.domain.Timeline;
import org.zalando.nakadi.exceptions.runtime.AccessDeniedException;
import org.zalando.nakadi.exceptions.runtime.EnrichmentException;
import org.zalando.nakadi.exceptions.runtime.EventTypeTimeoutException;
import org.zalando.nakadi.exceptions.runtime.InternalNakadiException;
import org.zalando.nakadi.exceptions.runtime.NoSuchEventTypeException;
import org.zalando.nakadi.exceptions.runtime.PartitioningException;
import org.zalando.nakadi.exceptions.runtime.PublishEventOwnershipException;
import org.zalando.nakadi.exceptions.runtime.ServiceTemporarilyUnavailableException;
import org.zalando.nakadi.service.TracingService;
import org.zalando.nakadi.service.publishing.check.Check;
import org.zalando.nakadi.service.timeline.TimelineService;
import org.zalando.nakadi.service.timeline.TimelineSync;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeoutException;

@Service
public class BinaryEventPublisher {

    private static final Logger LOG = LoggerFactory.getLogger(BinaryEventPublisher.class);

    private final TimelineService timelineService;
    private final TimelineSync timelineSync;
    private final NakadiSettings nakadiSettings;

    private final List<Check> prePublishingChecks;
    private final List<Check> preDeletingChecks;
    private final List<Check> internalPublishingChecks;

    @Autowired
    public BinaryEventPublisher(
            final TimelineService timelineService,
            final TimelineSync timelineSync,
            final NakadiSettings nakadiSettings,
            @Qualifier("pre-publishing-checks") final List<Check> prePublishingChecks,
            @Qualifier("pre-deleting-checks") final List<Check> preDeletingChecks,
            @Qualifier("internal-publishing-checks") final List<Check> internalPublishingChecks) {
        this.timelineService = timelineService;
        this.timelineSync = timelineSync;
        this.nakadiSettings = nakadiSettings;
        this.prePublishingChecks = prePublishingChecks;
        if (prePublishingChecks.isEmpty()) {
            // Safeguard against silent failure if spring inject an empty list
            throw new RuntimeException("prePublishingChecks should not be empty");
        }
        this.preDeletingChecks = preDeletingChecks;
        if (preDeletingChecks.isEmpty()) {
            // Safeguard against silent failure if spring inject an empty list
            throw new RuntimeException("preDeletingChecks should not be empty");
        }
        this.internalPublishingChecks = internalPublishingChecks;
        if (internalPublishingChecks.isEmpty()) {
            // Safeguard against silent failure if spring inject an empty list
            throw new RuntimeException("internalPublishingChecks should not be empty");
        }
    }

    public List<NakadiRecordResult> publish(final EventType eventType, final List<NakadiRecord> records) {
        return processInternal(eventType, records, prePublishingChecks);
    }

    private List<NakadiRecordResult> processInternal(final EventType eventType,
                                                     final List<NakadiRecord> records,
                                                     final List<Check> checks) {
        for (final Check check : checks) {
            final List<NakadiRecordResult> res = check.execute(eventType, records);
            if (res != null && !res.isEmpty()) {
                LOG.debug("Events sent to {} failed check {}; results are {}",
                        eventType.getName(), check.getClass().getName(), res);
                return res;
            }
        }
        if (records == null || records.isEmpty()) {
            throw new IllegalStateException("events have to be present when publishing");
        }
        Closeable publishingCloser = null;
        try {
            // publish under timeline lock
            publishingCloser = timelineSync.workWithEventType(
                    eventType.getName(),
                    nakadiSettings.getTimelineWaitTimeoutMs());
            final Timeline activeTimeline = timelineService.getActiveTimeline(eventType);
            final String topic = activeTimeline.getTopic();

            final Span publishingSpan = TracingService.buildNewSpan("publishing_to_kafka")
                    .withTag(Tags.MESSAGE_BUS_DESTINATION.getKey(), topic)
                    .withTag("event_type", eventType.getName())
                    .withTag("type", "binary")
                    .start();
            try (Closeable ignored = TracingService.activateSpan(publishingSpan)) {
                return timelineService.getTopicRepository(eventType).sendEvents(topic, records);
            } catch (final IOException ioe) {
                throw new InternalNakadiException("Error closing active span scope", ioe);
            } finally {
                publishingSpan.finish();
            }
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new InternalNakadiException("Failed to wait for timeline switch", e);
        } catch (final TimeoutException e) {
            throw new InternalNakadiException("Failed to wait for timeline switch", e);
        } finally {
            try {
                if (publishingCloser != null) {
                    publishingCloser.close();
                }
            } catch (final IOException e) {
                LOG.error("Exception occurred when releasing usage of event-type", e);
            }
        }
    }

    public List<NakadiRecordResult> delete(final List<NakadiRecord> events, final EventType eventType)
            throws NoSuchEventTypeException,
            InternalNakadiException,
            EnrichmentException,
            EventTypeTimeoutException,
            AccessDeniedException,
            PublishEventOwnershipException,
            ServiceTemporarilyUnavailableException,
            PartitioningException {
        LOG.debug("Deleting {} binary events from {}, with {} checks",
                events.size(), eventType.getName(), preDeletingChecks.size());
        return processInternal(eventType, events, preDeletingChecks);
    }

    public List<NakadiRecordResult> publishInternal(final EventType eventType, final List<NakadiRecord> events) {
        return processInternal(eventType, events, internalPublishingChecks);
    }
}