package org.zalando.nakadi.service.publishing;

import io.opentracing.Span;
import io.opentracing.tag.Tags;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.zalando.nakadi.cache.EventTypeCache;
import org.zalando.nakadi.config.NakadiSettings;
import org.zalando.nakadi.domain.EventType;
import org.zalando.nakadi.domain.NakadiRecord;
import org.zalando.nakadi.domain.NakadiRecordMetadata;
import org.zalando.nakadi.domain.Timeline;
import org.zalando.nakadi.exceptions.runtime.InternalNakadiException;
import org.zalando.nakadi.service.TracingService;
import org.zalando.nakadi.service.timeline.TimelineService;
import org.zalando.nakadi.service.timeline.TimelineSync;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeoutException;

@Service
public class AvroEventPublisher {

    private static final Logger LOG = LoggerFactory.getLogger(AvroEventPublisher.class);

    private final TimelineService timelineService;
    private final EventTypeCache eventTypeCache;
    private final TimelineSync timelineSync;
    private final NakadiSettings nakadiSettings;

    @Autowired
    public AvroEventPublisher(
            final TimelineService timelineService,
            final EventTypeCache eventTypeCache,
            final TimelineSync timelineSync,
            final NakadiSettings nakadiSettings) {
        this.timelineService = timelineService;
        this.eventTypeCache = eventTypeCache;
        this.timelineSync = timelineSync;
        this.nakadiSettings = nakadiSettings;
    }

    public List<NakadiRecordMetadata> publishAvro(final List<NakadiRecord> records) {
        if (records == null || records.isEmpty()) {
            throw new IllegalStateException("events have to be present when publishing");
        }

        Closeable publishingCloser = null;
        try {
            // assume everyting for the same event type
            final NakadiRecord nakadiRecord = records.get(0);
            final String eventTypeName = nakadiRecord.getEventType();

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
                    .withTag("type", "avro")
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
}