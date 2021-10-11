package org.zalando.nakadi.service.publishing;

import io.opentracing.Span;
import io.opentracing.tag.Tags;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.EncoderFactory;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.io.Resource;
import org.springframework.stereotype.Service;
import org.zalando.nakadi.cache.EventTypeCache;
import org.zalando.nakadi.config.NakadiSettings;
import org.zalando.nakadi.domain.CleanupPolicy;
import org.zalando.nakadi.domain.EventType;
import org.zalando.nakadi.domain.NakadiRecord;
import org.zalando.nakadi.domain.Timeline;
import org.zalando.nakadi.exceptions.runtime.EventPublishingException;
import org.zalando.nakadi.exceptions.runtime.InternalNakadiException;
import org.zalando.nakadi.partitioning.PartitionStrategy;
import org.zalando.nakadi.service.TracingService;
import org.zalando.nakadi.service.timeline.TimelineService;
import org.zalando.nakadi.service.timeline.TimelineSync;
import org.zalando.nakadi.util.FlowIdUtils;
import org.zalando.nakadi.util.UUIDGenerator;

import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeoutException;

@Service
public class AvroEventPublisher {

    private static final Logger LOG = LoggerFactory.getLogger(AvroEventPublisher.class);

    private final TimelineService timelineService;
    private final EventTypeCache eventTypeCache;
    private final TimelineSync timelineSync;
    private final NakadiSettings nakadiSettings;
    private final UUIDGenerator uuidGenerator;
    private final Schema eventSchema;
    private final Schema metadataSchema;

    @Autowired
    public AvroEventPublisher(
            final TimelineService timelineService,
            final EventTypeCache eventTypeCache,
            final TimelineSync timelineSync,
            final NakadiSettings nakadiSettings,
            final UUIDGenerator uuidGenerator,
            @Value("${nakadi.avro.access-log-schema:classpath:nakadi.access.log.avsc}") final Resource eventSchemaRes,
            @Value("${nakadi.avro.metadata-schema:classpath:nakadi.metadata.avsc}") final Resource metadataSchemaRes)
            throws IOException {
        this.timelineService = timelineService;
        this.eventTypeCache = eventTypeCache;
        this.timelineSync = timelineSync;
        this.nakadiSettings = nakadiSettings;
        this.uuidGenerator = uuidGenerator;
        // todo load from event type by name
        this.eventSchema = new Schema.Parser().parse(eventSchemaRes.getInputStream());
        this.metadataSchema = new Schema.Parser().parse(metadataSchemaRes.getInputStream());
    }

    public void publishAvro(final String etName,
                            final String method,
                            final String path,
                            final String query,
                            final String app,
                            final String hash,
                            final int statusCode,
                            final Long timeSpentMs) {
        try {
            final GenericRecord event = new GenericData.Record(eventSchema);
            event.put("method", method);
            event.put("path", path);
            event.put("query", query);
            event.put("app", app);
            event.put("app_hashed", hash);
            event.put("status_code", statusCode);
            event.put("response_time_ms", timeSpentMs);

            final String schemaVersion = eventTypeCache.getEventType(etName)
                    .getSchema().getVersion().toString();
            final GenericRecord metadata = new GenericData.Record(metadataSchema);
            metadata.put("occurred_at", new DateTime(DateTimeZone.UTC).toString());
            metadata.put("eid", uuidGenerator.randomUUID().toString());
            metadata.put("flow_id", FlowIdUtils.peek());
            metadata.put("event_type", etName);
            metadata.put("partition", 0); // fixme
            metadata.put("received_at", new DateTime(DateTimeZone.UTC).toString());
            metadata.put("version", schemaVersion); // fixme if it is not avro?
            metadata.put("published_by", app);

            event.put("metadata", metadata);

            publishUnderTimelineLock(etName, schemaVersion, metadata, event);
        } catch (final Exception e) {
            LOG.error("Error occurred when submitting KPI event for publishing", e);
        }
    }

    public void publishUnderTimelineLock(final String eventTypeName,
                                         final String schemaVersion,
                                         final GenericRecord metadata,
                                         final GenericRecord event) {
        Closeable publishingCloser = null;
        try {
            publishingCloser = timelineSync.workWithEventType(eventTypeName, nakadiSettings.getTimelineWaitTimeoutMs());

            // publish under timeline lock
            serializeAndSendToKafka(eventTypeName, schemaVersion, metadata, event);

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
    }

    private void serializeAndSendToKafka(final String eventTypeName,
                                         final String schemaVersion,
                                         final GenericRecord metadata,
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
            final ByteArrayOutputStream eventOutputStream = new ByteArrayOutputStream();
            final GenericDatumWriter genericDatumWriter = new GenericDatumWriter(eventSchema);
            genericDatumWriter.write(event, EncoderFactory.get()
                    .directBinaryEncoder(eventOutputStream, null));
            final byte[] eventKey = extractEventKey(eventType, metadata, event);

            timelineService.getTopicRepository(eventType)
                    // partition is null, kafka will assign partition
                    // org.apache.kafka.clients.producer.Partitioner
                    .syncPostEvent(new NakadiRecord(
                            eventTypeName,
                            schemaVersion,
                            topic,
                            eventKey,
                            eventOutputStream.toByteArray()));
        } catch (final EventPublishingException epe) {
            publishingSpan.log(epe.getMessage());
            throw epe;
        } catch (final IOException ioe) {
            throw new InternalNakadiException("Error closing active span scope", ioe);
        } finally {
            publishingSpan.finish();
        }
    }

    private byte[] extractEventKey(final EventType eventType,
                                   final GenericRecord metadata,
                                   final GenericRecord event) {
        if (eventType.getCleanupPolicy() == CleanupPolicy.COMPACT ||
                eventType.getCleanupPolicy() == CleanupPolicy.COMPACT_AND_DELETE) {
            return ((String) metadata.get("partition_compaction_key"))
                    .getBytes(StandardCharsets.UTF_8);
        }

        if (PartitionStrategy.HASH_STRATEGY.equals(eventType.getPartitionStrategy())) {
            // todo
        }

        return null;
    }
}
