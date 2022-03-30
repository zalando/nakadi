package org.zalando.nakadi.service.publishing;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.json.JSONObject;
import org.springframework.stereotype.Component;
import org.zalando.nakadi.util.FlowIdUtils;
import org.zalando.nakadi.util.UUIDGenerator;

import java.time.Instant;
import java.util.UUID;

@Component
public class EventMetadata {
    public static final String METADATA_FIELD = "metadata";
    public static final String OCCURRED_AT_FIELD = "occurred_at";
    public static final String EID_FIELD = "eid";
    public static final String FLOW_ID_FIELD = "flow_id";
    public static final String RECEIVED_AT_FIELD = "received_at";
    public static final String SCHEMA_VERSION_FIELD = "schema_version";
    public static final String PUBLISHED_BY_FIELD = "published_by";
    public static final String EVENT_TYPE_FIELD = "event_type";
    public static final String PARTITION_FIELD = "partition";

    private final UUIDGenerator uuidGenerator;

    public EventMetadata(final UUIDGenerator uuidGenerator) {
        this.uuidGenerator = uuidGenerator;
    }

    @Deprecated
    public JSONObject addTo(final JSONObject event) {
        return event.put(METADATA_FIELD, new JSONObject()
                .put("occurred_at", Instant.now())
                .put("eid", uuidGenerator.randomUUID())
                .put("flow_id", FlowIdUtils.peek()));
    }

    public Builder generateMetadata() {
        return new Builder()
                .setOccurredAt(Instant.now())
                .setEid(uuidGenerator.randomUUID())
                .setFlowId(FlowIdUtils.peek());
    }

    public static class Builder {
        private Instant occurredAt;
        private UUID eid;
        private String flowId;
        private Instant receivedAt;
        private String schemaVersion;
        private String publishedBy;
        private String eventType;
        private Integer partition;

        public Instant getOccurredAt() {
            return occurredAt;
        }

        public Builder setOccurredAt(final Instant occurredAt) {
            this.occurredAt = occurredAt;
            return this;
        }

        public UUID getEid() {
            return eid;
        }

        public Builder setEid(final UUID eid) {
            this.eid = eid;
            return this;
        }

        public String getFlowId() {
            return flowId;
        }

        public Builder setFlowId(final String flowId) {
            this.flowId = flowId;
            return this;
        }

        public Instant getReceivedAt() {
            return receivedAt;
        }

        public Builder setReceivedAt(final Instant receivedAt) {
            this.receivedAt = receivedAt;
            return this;
        }

        public String getSchemaVersion() {
            return schemaVersion;
        }

        public Builder setSchemaVersion(final String schemaVersion) {
            this.schemaVersion = schemaVersion;
            return this;
        }

        public String getPublishedBy() {
            return publishedBy;
        }

        public Builder setPublishedBy(final String publishedBy) {
            this.publishedBy = publishedBy;
            return this;
        }

        public String getEventType() {
            return eventType;
        }

        public Builder setEventType(final String eventType) {
            this.eventType = eventType;
            return this;
        }

        public Integer getPartition() {
            return partition;
        }

        public Builder setPartition(final Integer partition) {
            this.partition = partition;
            return this;
        }

        public JSONObject asJson() {
            return new JSONObject()
                    .put(OCCURRED_AT_FIELD, occurredAt)
                    .put(EID_FIELD, eid)
                    .put(FLOW_ID_FIELD, flowId)
                    .put(RECEIVED_AT_FIELD, receivedAt)
                    .put(SCHEMA_VERSION_FIELD, schemaVersion)
                    .put(PUBLISHED_BY_FIELD, publishedBy)
                    .put(EVENT_TYPE_FIELD, eventType)
                    .put(PARTITION_FIELD, partition);
        }

        public GenericRecord asAvroGenericRecord(final Schema schema) {

            return new GenericRecordBuilder(schema)
                    .set(OCCURRED_AT_FIELD, occurredAt.toEpochMilli())
                    .set(EID_FIELD, eid.toString())
                    .set(FLOW_ID_FIELD, flowId)
                    .set(EVENT_TYPE_FIELD, eventType)
                    .set(PARTITION_FIELD, partition)
                    .set(RECEIVED_AT_FIELD, receivedAt.toEpochMilli())
                    .set(SCHEMA_VERSION_FIELD, schemaVersion)
                    .set(PUBLISHED_BY_FIELD, publishedBy)
                    .build();
        }
    }
}
