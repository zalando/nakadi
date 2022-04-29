package org.zalando.nakadi.domain;

import org.apache.avro.generic.GenericRecord;

public class GenericRecordMetadata implements NakadiMetadata {

    private static final String EID = "eid";
    private static final String OCCURRED_AT = "occurred_at";
    private static final String PUBLISHED_BY = "published_by";
    private static final String RECEIVED_AT = "received_at";
    private static final String EVENT_TYPE = "event_type";
    private static final String FLOW_ID = "flow_id";
    private static final String SCHEMA_VERSION = "schema_version";
    private static final String PARTITION = "partition";
    private final GenericRecord metadata;
    private final byte metadataVersion;

    public GenericRecordMetadata(final GenericRecord metadata,
                                 final byte metadataVersion) {
        this.metadata = metadata;
        this.metadataVersion = metadataVersion;
    }

    public byte getMetadataVersion() {
        return this.metadataVersion;
    }

    @Override
    public String getEid() {
        return this.metadata.get(EID).toString();
    }

    @Override
    public String getEventType() {
        return this.metadata.get(EVENT_TYPE).toString();
    }

    @Override
    public void setEventType(final String eventType) {
        this.metadata.put(EVENT_TYPE, eventType);
    }

    @Override
    public void setPartition(final String partition) {
        this.metadata.put(PARTITION, partition);
    }

    @Override
    public String getPartitionStr() {
        final Object partition = this.metadata.get(PARTITION);
        if (partition == null) {
            return null;
        }
        return partition.toString();
    }

    @Override
    public Integer getPartitionInt() {
        final Object partition = this.metadata.get(PARTITION);
        if (partition == null) {
            return null;
        }
        return Integer.valueOf(partition.toString());
    }

    @Override
    public String getOccurredAt() {
        return this.metadata.get(OCCURRED_AT).toString();
    }

    @Override
    public String getPublishedBy() {
        return this.metadata.get(PUBLISHED_BY).toString();
    }

    @Override
    public void setPublishedBy(final String publisher) {
        this.metadata.put(PUBLISHED_BY, publisher);
    }

    @Override
    public String getReceivedAt() {
        return this.metadata.get(RECEIVED_AT).toString();
    }

    @Override
    public void setReceivedAt(final String receivedAt) {
        this.metadata.put(RECEIVED_AT, receivedAt);
    }

    @Override
    public String getFlowId() {
        return this.metadata.get(FLOW_ID).toString();
    }

    @Override
    public void setFlowId(final String flowId) {
        this.metadata.put(FLOW_ID, flowId);
    }

    @Override
    public String getSchemaVersion() {
        return this.metadata.get(SCHEMA_VERSION).toString();
    }

    @Override
    public void setSchemaVersion(final String schemaVersion) {
        this.metadata.put(SCHEMA_VERSION, schemaVersion);
    }
}
