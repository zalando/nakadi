package org.zalando.nakadi.domain;

public class NakadiRecord {

    public static final String HEADER_EVENT_TYPE = "X-Event-Type";
    public static final String HEADER_SCHEMA_TYPE = "X-Schema-Type";
    public static final String HEADER_SCHEMA_VERSION = "X-Schema-Version";

    private final String eventType;
    private final String topic;
    private final Integer partition;
    private final byte[] eventKey;
    private final byte[] data;
    private final String schemaType;
    private final String schemaVersion;

    public NakadiRecord(
            final String eventType,
            final String topic,
            final byte[] eventKey,
            final byte[] data) {
        this.topic = topic;
        this.partition = null;
        this.eventKey = eventKey;
        this.data = data;
        this.schemaType = "avro";
        this.eventType = eventType;
        this.schemaVersion = "v1";
    }

    public String getTopic() {
        return topic;
    }

    public Integer getPartition() {
        return partition;
    }

    public byte[] getEventKey() {
        return eventKey;
    }

    public byte[] getData() {
        return data;
    }

    public String getEventType() {
        return eventType;
    }

    public String getSchemaType() {
        return schemaType;
    }

    public String getSchemaVersion() {
        return schemaVersion;
    }
}
