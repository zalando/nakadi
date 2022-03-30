package org.zalando.nakadi.service.publishing;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.zalando.nakadi.config.KPIEventTypes;
import org.zalando.nakadi.domain.Feature;
import org.zalando.nakadi.domain.NakadiRecord;
import org.zalando.nakadi.security.UsernameHasher;
import org.zalando.nakadi.service.AvroSchema;
import org.zalando.nakadi.service.FeatureToggleService;
import org.zalando.nakadi.util.UUIDGenerator;

import java.time.Instant;
import java.util.Map;
import java.util.function.Supplier;

@Component
public class NakadiKpiPublisher {

    private static final Logger LOG = LoggerFactory.getLogger(NakadiKpiPublisher.class);

    private final FeatureToggleService featureToggleService;
    private final JsonEventProcessor jsonEventsProcessor;
    private final BinaryEventProcessor binaryEventsProcessor;
    private final UsernameHasher usernameHasher;
    private final EventMetadata eventMetadata;
    private final UUIDGenerator uuidGenerator;
    private final AvroSchema avroSchema;

    @Autowired
    protected NakadiKpiPublisher(
            final FeatureToggleService featureToggleService,
            final JsonEventProcessor jsonEventsProcessor,
            final BinaryEventProcessor binaryEventsProcessor,
            final UsernameHasher usernameHasher,
            final EventMetadata eventMetadata,
            final UUIDGenerator uuidGenerator,
            final AvroSchema avroSchema) {
        this.featureToggleService = featureToggleService;
        this.jsonEventsProcessor = jsonEventsProcessor;
        this.binaryEventsProcessor = binaryEventsProcessor;
        this.usernameHasher = usernameHasher;
        this.eventMetadata = eventMetadata;
        this.uuidGenerator = uuidGenerator;
        this.avroSchema = avroSchema;
    }

    public void publish(final String etName, final Supplier<JSONObject> eventSupplier) {
        try {
            if (!featureToggleService.isFeatureEnabled(Feature.KPI_COLLECTION)) {
                return;
            }
            final var event = eventSupplier.get();
            event.put(EventMetadata.METADATA_FIELD, eventMetadata.generateMetadata().asJson());
            jsonEventsProcessor.queueEvent(etName, event);
        } catch (final Exception e) {
            LOG.error("Error occurred when submitting KPI event for publishing", e);
        }
    }

    public void publishNakadiBatchPublishedEvent(
            final String eventTypeName,
            final String applicationName,
            final String tokenRealm,
            final int eventCount,
            final long msSpent,
            final int totalSizeBytes
    ) {
        if (!featureToggleService.isFeatureEnabled(Feature.AVRO_FOR_KPI_EVENTS)) {
            publish(KPIEventTypes.BATCH_PUBLISHED, () -> new JSONObject()
                    .put("event_type", eventTypeName)
                    .put("app", applicationName)
                    .put("app_hashed", hash(applicationName))
                    .put("token_realm", tokenRealm)
                    .put("number_of_events", eventCount)
                    .put("ms_spent", msSpent)
                    .put("batch_size", totalSizeBytes));
            return;
        }
        try {
            final Map.Entry<String, Schema> latestMeta =
                    avroSchema.getLatestEventTypeSchemaVersion(AvroSchema.METADATA_KEY);
            final byte metadataVersion = Byte.parseByte(latestMeta.getKey());

            final Map.Entry<String, Schema> latestSchema =
                    avroSchema.getLatestEventTypeSchemaVersion(KPIEventTypes.BATCH_PUBLISHED);

            final GenericRecord metadata = buildMetaDataGenericRecord(
                    KPIEventTypes.BATCH_PUBLISHED, latestMeta.getValue(), latestSchema.getKey());

            final GenericRecord event = new GenericRecordBuilder(latestSchema.getValue())
                    .set("event_type", eventTypeName)
                    .set("app", applicationName)
                    .set("app_hashed", hash(applicationName))
                    .set("token_realm", tokenRealm)
                    .set("number_of_events", eventCount)
                    .set("ms_spent", msSpent)
                    .set("batch_size", totalSizeBytes)
                    .build();

            final NakadiRecord nakadiRecord = NakadiRecord
                    .fromAvro(KPIEventTypes.BATCH_PUBLISHED, metadataVersion, metadata, event);
            binaryEventsProcessor.queueEvent(KPIEventTypes.BATCH_PUBLISHED, nakadiRecord);
        } catch (final Exception e) {
            LOG.error("Error occurred when submitting KPI event for publishing", e);
        }
    }

    public void publishAccessLogEvent(final String method,
                                      final String path,
                                      final String query,
                                      final String userAgent,
                                      final String user,
                                      final String contentEncoding,
                                      final String acceptEncoding,
                                      final int statusCode,
                                      final Long timeSpentMs,
                                      final Long requestLength,
                                      final Long responseLength) {
        try {
            if (!featureToggleService.isFeatureEnabled(Feature.AVRO_FOR_KPI_EVENTS)) {
                publish(KPIEventTypes.ACCESS_LOG, () -> new JSONObject()
                        .put("method", method)
                        .put("path", path)
                        .put("query", query)
                        .put("user_agent", userAgent)
                        .put("app", user)
                        .put("accept_encoding", acceptEncoding)
                        .put("content_encoding", contentEncoding)
                        .put("app_hashed", hash(user))
                        .put("status_code", statusCode)
                        .put("response_time_ms", timeSpentMs)
                        .put("request_length", requestLength)
                        .put("response_length", responseLength));
                return;
            }

            final Map.Entry<String, Schema> latestMeta =
                    avroSchema.getLatestEventTypeSchemaVersion(AvroSchema.METADATA_KEY);
            final byte metadataVersion = Byte.parseByte(latestMeta.getKey());

            final Map.Entry<String, Schema> latestSchema =
                    avroSchema.getLatestEventTypeSchemaVersion(KPIEventTypes.ACCESS_LOG);

            final GenericRecord metadata = buildMetaDataGenericRecord(
                    KPIEventTypes.ACCESS_LOG, latestMeta.getValue(), latestSchema.getKey(), user);


            final GenericRecord event = new GenericRecordBuilder(latestSchema.getValue())
                    .set("method", method)
                    .set("path", path)
                    .set("query", query)
                    .set("user_agent", userAgent)
                    .set("app", user)
                    .set("app_hashed", hash(user))
                    .set("status_code", statusCode)
                    .set("response_time_ms", timeSpentMs)
                    .set("accept_encoding", acceptEncoding)
                    .set("content_encoding", contentEncoding)
                    .set("request_length", requestLength)
                    .set("response_length", responseLength)
                    .build();

            final NakadiRecord nakadiRecord = NakadiRecord
                    .fromAvro(KPIEventTypes.ACCESS_LOG, metadataVersion, metadata, event);
            binaryEventsProcessor.queueEvent(KPIEventTypes.ACCESS_LOG, nakadiRecord);
        } catch (final Exception e) {
            LOG.error("Error occurred when submitting KPI event for publishing", e);
        }
    }


    private GenericRecord buildMetaDataGenericRecord(
            final String eventType, final Schema schema, final String version) {
        return buildMetaDataGenericRecord(eventType, schema, version, "unknown");
    }

    private GenericRecord buildMetaDataGenericRecord(
            final String eventType, final Schema schema, final String version, final String user) {
        return eventMetadata.generateMetadata()
                .setEventType(eventType)
                .setPartition(0) // fixme avro
                .setReceivedAt(Instant.now())
                .setSchemaVersion(version)
                .setPublishedBy(user)
                .asAvroGenericRecord(schema);
    }

    public String hash(final String value) {
        return usernameHasher.hash(value);
    }
}
