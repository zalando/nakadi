package org.zalando.nakadi.domain;

public class NakadiRecordResult {

    public enum Status {
        SUCCEEDED, FAILED, NOT_ATTEMPTED
    }

    private final NakadiMetadata metadata;
    private final Status status;
    private final Exception exception;

    public NakadiRecordResult(
            final NakadiMetadata metadata,
            final Status status,
            final Exception exception) {
        this.metadata = metadata;
        this.status = status;
        this.exception = exception;
    }

    public NakadiRecordResult(
            final NakadiMetadata metadata,
            final Status status) {
        this(metadata, status, null);
    }

    public NakadiMetadata getMetadata() {
        return metadata;
    }

    public Status getStatus() {
        return status;
    }

    public Exception getException() {
        return exception;
    }
}
