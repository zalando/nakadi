package org.zalando.nakadi.domain;

public class NakadiRecordResult {

    public enum Status {
        SUCCEEDED, FAILED, ABORTED
    }

    public enum Step {
        NONE,
        VALIDATION,
        PARTITIONING,
        ENRICHMENT,
        PUBLISHING
    }

    private final NakadiMetadata metadata;
    private final Status status;
    private final Step step;
    private final Exception exception;

    public NakadiRecordResult(
            final NakadiMetadata metadata,
            final Status status,
            final Step step,
            final Exception exception) {
        this.metadata = metadata;
        this.status = status;
        this.step = step;
        this.exception = exception;
    }

    public NakadiRecordResult(
            final NakadiMetadata metadata,
            final Status status,
            final Step step) {
        this(metadata, status, step, null);
    }

    public NakadiMetadata getMetadata() {
        return metadata;
    }

    public Status getStatus() {
        return status;
    }

    public Step getStep() {
        return step;
    }

    public Exception getException() {
        return exception;
    }
}
