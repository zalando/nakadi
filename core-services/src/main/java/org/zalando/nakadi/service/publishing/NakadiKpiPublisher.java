package org.zalando.nakadi.service.publishing;

import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.zalando.nakadi.domain.Feature;
import org.zalando.nakadi.domain.NakadiRecord;
import org.zalando.nakadi.security.UsernameHasher;
import org.zalando.nakadi.service.AvroSchema;
import org.zalando.nakadi.service.FeatureToggleService;
import org.zalando.nakadi.util.FlowIdUtils;
import org.zalando.nakadi.util.UUIDGenerator;

import java.util.function.Supplier;

@Component
public class NakadiKpiPublisher {

    private static final Logger LOG = LoggerFactory.getLogger(NakadiKpiPublisher.class);

    private final FeatureToggleService featureToggleService;
    private final JsonEventProcessor jsonEventsProcessor;
    private final AvroEventProcessor avroEventsProcessor;
    private final UsernameHasher usernameHasher;
    private final EventMetadata eventMetadata;
    private final UUIDGenerator uuidGenerator;
    private final AvroSchema avroSchema;

    @Autowired
    protected NakadiKpiPublisher(final FeatureToggleService featureToggleService,
                                 final JsonEventProcessor jsonEventsProcessor,
                                 final AvroEventProcessor avroEventsProcessor,
                                 final UsernameHasher usernameHasher,
                                 final EventMetadata eventMetadata,
                                 final UUIDGenerator uuidGenerator,
                                 final AvroSchema avroSchema) {
        this.featureToggleService = featureToggleService;
        this.jsonEventsProcessor = jsonEventsProcessor;
        this.avroEventsProcessor = avroEventsProcessor;
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

            jsonEventsProcessor.queueEvent(etName, eventMetadata.addTo(eventSupplier.get()));
        } catch (final Exception e) {
            LOG.error("Error occurred when submitting KPI event for publishing", e);
        }
    }

    public void publish(final String eventTypeName,
                        final String method,
                        final String path,
                        final String query,
                        final String user,
                        final int statusCode,
                        final Long timeSpentMs) {
        try {
            final long now = System.currentTimeMillis();
            final GenericRecord metadata = new GenericRecordBuilder(
                    avroSchema.getMetadataSchema())
                    .set("occurred_at", now)
                    .set("eid", uuidGenerator.randomUUID().toString())
                    .set("flow_id", FlowIdUtils.peek())
                    .set("event_type", eventTypeName)
                    .set("partition", 0) // fixme avro
                    .set("received_at", now)
                    .set("schema_version", "0")  // fixme avro
                    .set("published_by", user)
                    .build();
            final GenericRecord event = new GenericRecordBuilder(
                    avroSchema.getNakadiAccessLogSchema())
                    .set("method", method)
                    .set("path", path)
                    .set("query", query)
                    .set("app", user)
                    .set("app_hashed", hash(user))
                    .set("status_code", statusCode)
                    .set("response_time_ms", timeSpentMs)
                    .build();

            final NakadiRecord nakadiRecord = NakadiRecord
                    .fromAvro(eventTypeName, metadata, event);
            avroEventsProcessor.queueEvent(eventTypeName, nakadiRecord);
        } catch (final Exception e) {
            LOG.error("Error occurred when submitting KPI event for publishing", e);
        }
    }

    public String hash(final String value) {
        return usernameHasher.hash(value);
    }
}
