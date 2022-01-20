package org.zalando.nakadi.domain;

public class NakadiRecord {

    public static final String HEADER_FORMAT = new String(new byte[]{0});

    public enum Format {
        AVRO(new byte[]{0});

        private final byte[] format;

        Format(final byte[] format) {
            this.format = format;
        }

        public byte[] getFormat() {
            return this.format;
        }
    }

    private final String eventType;
    private final String topic;
    private final Integer partition;
    private final byte[] eventKey;
    private final byte[] data;
    private final byte[] format;

    public NakadiRecord(
            final String eventType,
            final String topic,
            final Integer partition,
            final byte[] format,
            final byte[] eventKey,
            final byte[] data) {
        this.topic = topic;
        this.partition = partition;
        this.eventKey = eventKey;
        this.data = data;
        this.eventType = eventType;
        this.format = format;
    }

    public String getEventType() {
        return eventType;
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

    public byte[] getFormat() {
        return format;
    }
}
