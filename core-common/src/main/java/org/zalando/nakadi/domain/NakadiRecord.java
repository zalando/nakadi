package org.zalando.nakadi.domain;

import java.nio.charset.StandardCharsets;

public class NakadiRecord {

    public static final String HEADER_EVENT_TYPE = "X-Event-Type";
    public static final String HEADER_SCHEMA_TYPE = "X-Schema-Type";
    public static final String HEADER_SCHEMA_VERSION = "X-Schema-Version";

    public static final byte[] SCHEMA_TYPE_AVRO = "avro".getBytes(StandardCharsets.UTF_8);

    private final String eventType;
    private final String topic;
    private final Integer partition;
    private final byte[] eventKey;
    private final byte[] data;
    private final byte[] schemaType;
    private final byte[] schemaVersion;

    public NakadiRecord(
            final String eventType,
            final String version,
            final String topic,
            final byte[] eventKey,
            final byte[] data) {
        this.topic = topic;
        this.partition = null;
        this.eventKey = eventKey;
        this.data = data;
        this.schemaType = SCHEMA_TYPE_AVRO;
        this.eventType = eventType;
        this.schemaVersion = version.getBytes(StandardCharsets.UTF_8);
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

    public byte[] getSchemaType() {
        return schemaType;
    }

    public byte[] getSchemaVersion() {
        return schemaVersion;
    }
}
