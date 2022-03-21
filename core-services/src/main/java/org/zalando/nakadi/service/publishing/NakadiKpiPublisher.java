package org.zalando.nakadi.service.publishing;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.zalando.nakadi.domain.Feature;
import org.zalando.nakadi.domain.NakadiRecord;
import org.zalando.nakadi.security.UsernameHasher;
import org.zalando.nakadi.service.AvroSchema;
import org.zalando.nakadi.service.FeatureToggleService;
import org.zalando.nakadi.util.FlowIdUtils;
import org.zalando.nakadi.util.UUIDGenerator;

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
    private final String accessLogEventType;

    @Autowired
    protected NakadiKpiPublisher(
            final FeatureToggleService featureToggleService,
            final JsonEventProcessor jsonEventsProcessor,
            final BinaryEventProcessor binaryEventsProcessor,
            final UsernameHasher usernameHasher,
            final EventMetadata eventMetadata,
            final UUIDGenerator uuidGenerator,
            final AvroSchema avroSchema,
            @Value("${nakadi.kpi.event-types.nakadiAccessLog}") final String accessLogEventType) {
        this.featureToggleService = featureToggleService;
        this.jsonEventsProcessor = jsonEventsProcessor;
        this.binaryEventsProcessor = binaryEventsProcessor;
        this.usernameHasher = usernameHasher;
        this.eventMetadata = eventMetadata;
        this.uuidGenerator = uuidGenerator;
        this.avroSchema = avroSchema;
        this.accessLogEventType = accessLogEventType;
    }

    public void publish(final String etName, final Supplier<JSONObject> eventSupplier) {
        try {
            if (!featureToggleService.isFeatureEnabled(Feature.KPI_COLLECTION)) {
                return;
            }

            jsonEventsProcessor.queueEvent(etName, eventMetadata.addTo(eventSupplier.get()));
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
                publish(accessLogEventType, () -> new JSONObject()
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

            final long now = System.currentTimeMillis();
            final Map.Entry<String, Schema> latestSchema =
                    avroSchema.getLatestEventTypeSchemaVersion(accessLogEventType);

            final GenericRecord metadata = new GenericRecordBuilder(avroSchema.getMetadataSchema())
                    .set("occurred_at", now)
                    .set("eid", uuidGenerator.randomUUID().toString())
                    .set("flow_id", FlowIdUtils.peek())
                    .set("event_type", accessLogEventType)
                    .set("partition", 0) // fixme avro
                    .set("received_at", now)
                    .set("schema_version", latestSchema.getKey())
                    .set("published_by", user)
                    .build();
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
                    .fromAvro(accessLogEventType, metadata, event);
            binaryEventsProcessor.queueEvent(accessLogEventType, nakadiRecord);
        } catch (final Exception e) {
            LOG.error("Error occurred when submitting KPI event for publishing", e);
        }
    }

    public String hash(final String value) {
        return usernameHasher.hash(value);
    }
}
