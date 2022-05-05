package org.zalando.nakadi.service.publishing;

import io.opentracing.Span;
import io.opentracing.tag.Tags;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;
import org.zalando.nakadi.cache.EventTypeCache;
import org.zalando.nakadi.config.NakadiSettings;
import org.zalando.nakadi.domain.EventType;
import org.zalando.nakadi.domain.NakadiRecord;
import org.zalando.nakadi.domain.NakadiRecordResult;
import org.zalando.nakadi.domain.Timeline;
import org.zalando.nakadi.exceptions.runtime.InternalNakadiException;
import org.zalando.nakadi.exceptions.runtime.PartitioningException;
import org.zalando.nakadi.partitioning.PartitionResolver;
import org.zalando.nakadi.service.AuthorizationValidator;
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
    private final EventTypeCache eventTypeCache;
    private final TimelineSync timelineSync;
    private final NakadiSettings nakadiSettings;
    private final AuthorizationValidator authValidator;
    private final List<Check> prePublishingChecks;
    private final PartitionResolver partitionResolver;

    @Autowired
    public BinaryEventPublisher(
            final TimelineService timelineService,
            final EventTypeCache eventTypeCache,
            final TimelineSync timelineSync,
            final NakadiSettings nakadiSettings,
            final AuthorizationValidator authValidator,
            @Qualifier("pre-publishing-checks") final List<Check> prePublishingChecks,
            final PartitionResolver partitionResolver) {
        this.timelineService = timelineService;
        this.eventTypeCache = eventTypeCache;
        this.timelineSync = timelineSync;
        this.nakadiSettings = nakadiSettings;
        this.authValidator = authValidator;
        this.prePublishingChecks = prePublishingChecks;
        this.partitionResolver = partitionResolver;
    }

    public List<NakadiRecordResult> publish(final String eventTypeName,
                                            final List<NakadiRecord> records) {
        if (records == null || records.isEmpty()) {
            throw new IllegalStateException("events have to be present when publishing");
        }

        Closeable publishingCloser = null;
        try {
            // publish under timeline lock
            publishingCloser = timelineSync.workWithEventType(
                    eventTypeName,
                    nakadiSettings.getTimelineWaitTimeoutMs());
            final EventType eventType = eventTypeCache.getEventType(eventTypeName);
            final Timeline activeTimeline = timelineService.getActiveTimeline(eventType);
            final String topic = activeTimeline.getTopic();
            final Span publishingSpan = TracingService
                    .buildNewSpan("publishing_to_kafka")
                    .withTag(Tags.MESSAGE_BUS_DESTINATION.getKey(), topic)
                    .withTag("event_type", eventTypeName)
                    .withTag("type", "binary")
                    .start();
            partition(eventType, records);
            System.out.println(records.get(0).getMetadata().getPartitionStr());
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

    public List<Check.RecordResult> checkEvents(final String eventTypeName,
                                                final List<NakadiRecord> records) {
        final EventType eventType = eventTypeCache.getEventType(eventTypeName);

        // throws exception, maybe move it somewhere else
        authValidator.authorizeEventTypeWrite(eventType);

        for (final Check check : prePublishingChecks) {
            final List<Check.RecordResult> res = check.execute(eventType, records);
            if (res != null) {
                return res;
            }
        }

        return null;
    }

    private void partition(final EventType eventType, final List<NakadiRecord> records)
            throws PartitioningException {
        for (final var record : records) {
            final var metadata = record.getMetadata();
            final var partition = this.partitionResolver.resolvePartition(eventType, metadata);

            metadata.setPartition(partition);
        }
    }
}