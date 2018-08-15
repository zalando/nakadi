package org.zalando.nakadi.exceptions.runtime;

public class PartitioningException extends NakadiRuntimeBaseException {
    public PartitioningException(final String message) {
        super(message);
    }

    public PartitioningException(final String msg, final Exception cause) {
        super(msg, cause);
    }
}
