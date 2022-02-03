package org.zalando.nakadi.domain;

public class NakadiRecordMetadata {

    private final String eventType;
    private final Integer partition;
    private final byte[] metadata;
    private final Exception exception;

    public NakadiRecordMetadata(
            final String eventType,
            final Integer partition,
            final byte[] metadata,
            final Exception exception) {
        this.eventType = eventType;
        this.partition = partition;
        this.metadata = metadata;
        this.exception = exception;
    }

    public String getEventType() {
        return eventType;
    }

    public Integer getPartition() {
        return partition;
    }

    public byte[] getMetadata() {
        return metadata;
    }

    public Exception getException() {
        return exception;
    }
}
