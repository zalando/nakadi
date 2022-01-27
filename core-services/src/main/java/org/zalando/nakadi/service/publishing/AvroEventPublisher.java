package org.zalando.nakadi.service.publishing;

import io.opentracing.Span;
import io.opentracing.tag.Tags;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.EncoderFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.zalando.nakadi.cache.EventTypeCache;
import org.zalando.nakadi.config.NakadiSettings;
import org.zalando.nakadi.domain.EnvelopeHolder;
import org.zalando.nakadi.domain.EventType;
import org.zalando.nakadi.domain.NakadiRecord;
import org.zalando.nakadi.domain.Timeline;
import org.zalando.nakadi.exceptions.runtime.EventPublishingException;
import org.zalando.nakadi.exceptions.runtime.InternalNakadiException;
import org.zalando.nakadi.service.AvroSchema;
import org.zalando.nakadi.service.TracingService;
import org.zalando.nakadi.service.timeline.TimelineService;
import org.zalando.nakadi.service.timeline.TimelineSync;
import org.zalando.nakadi.util.FlowIdUtils;
import org.zalando.nakadi.util.UUIDGenerator;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.TimeoutException;

@Service
public class AvroEventPublisher {

    private static final Logger LOG = LoggerFactory.getLogger(AvroEventPublisher.class);

    private final TimelineService timelineService;
    private final EventTypeCache eventTypeCache;
    private final TimelineSync timelineSync;
    private final NakadiSettings nakadiSettings;
    private final UUIDGenerator uuidGenerator;
    private final AvroSchema avroSchema;

    @Autowired
    public AvroEventPublisher(
            final TimelineService timelineService,
            final EventTypeCache eventTypeCache,
            final TimelineSync timelineSync,
            final NakadiSettings nakadiSettings,
            final UUIDGenerator uuidGenerator,
            final AvroSchema avroSchema) {
        this.timelineService = timelineService;
        this.eventTypeCache = eventTypeCache;
        this.timelineSync = timelineSync;
        this.nakadiSettings = nakadiSettings;
        this.uuidGenerator = uuidGenerator;
        this.avroSchema = avroSchema;
    }

    public void publishAvro(final String etName,
                            final String publishedBy,
                            final GenericRecord event) {
        try {
            final GenericRecord metadata = new GenericData.Record(avroSchema.getMetadataSchema());
            final long now = System.currentTimeMillis();
            metadata.put("occurred_at", now);
            metadata.put("eid", uuidGenerator.randomUUID().toString());
            metadata.put("flow_id", FlowIdUtils.peek());
            metadata.put("event_type", etName);
            metadata.put("partition", 0); // fixme avro
            metadata.put("received_at", now);
            metadata.put("schema_version", "0");  // fixme avro
            metadata.put("published_by", publishedBy);

            Closeable publishingCloser = null;
            try {
                publishingCloser = timelineSync.workWithEventType(etName, nakadiSettings.getTimelineWaitTimeoutMs());

                // publish under timeline lock
                serializeAndSendToKafka(etName, metadata, event);

            } catch (final InterruptedException e) {
                Thread.currentThread().interrupt();
                LOG.error("Failed to wait for timeline switch", e);
            } catch (final TimeoutException e) {
                LOG.error("Failed to wait for timeline switch", e);
            } finally {
                try {
                    if (publishingCloser != null) {
                        publishingCloser.close();
                    }
                } catch (final IOException e) {
                    LOG.error("Exception occurred when releasing usage of event-type", e);
                }
            }

        } catch (final Exception e) {
            LOG.error("Error occurred when submitting KPI event for publishing", e);
        }
    }

    private void serializeAndSendToKafka(final String eventTypeName,
                                         final GenericRecord eventMetadata,
                                         final GenericRecord event) {
        final EventType eventType = eventTypeCache.getEventType(eventTypeName);
        final Timeline activeTimeline = timelineService.getActiveTimeline(eventType);
        final String topic = activeTimeline.getTopic();

        final Span publishingSpan = TracingService.buildNewSpan("publishing_to_kafka")
                .withTag(Tags.MESSAGE_BUS_DESTINATION.getKey(), topic)
                .withTag("event_type", eventTypeName)
                .withTag("type", "avro")
                .start();

        try (Closeable ignored = TracingService.activateSpan(publishingSpan)) {
            final byte[] data = EnvelopeHolder.produceBytes(
                    AvroSchema.METADATA_VERSION,
                    (outputStream -> {
                        final GenericDatumWriter eventWriter = new GenericDatumWriter(eventMetadata.getSchema());
                        eventWriter.write(eventMetadata, EncoderFactory.get()
                                .directBinaryEncoder(outputStream, null));
                    }),
                    (outputStream -> {
                        final GenericDatumWriter eventWriter = new GenericDatumWriter(event.getSchema());
                        eventWriter.write(event, EncoderFactory.get()
                                .directBinaryEncoder(outputStream, null));
                    }));

            timelineService.getTopicRepository(eventType)
                    .syncPostEvent(new NakadiRecord(
                            eventTypeName,
                            topic,
                            // partition is null, kafka will assign partition
                            // org.apache.kafka.clients.producer.Partitioner
                            null,
                            NakadiRecord.Format.AVRO.getFormat(),
                            null,
                            data));
        } catch (final EventPublishingException epe) {
            publishingSpan.log(epe.getMessage());
            throw epe;
        } catch (final IOException ioe) {
            throw new InternalNakadiException("Error closing active span scope", ioe);
        } finally {
            publishingSpan.finish();
        }
    }
}