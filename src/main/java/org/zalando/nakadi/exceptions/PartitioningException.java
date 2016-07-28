package org.zalando.nakadi.exceptions;

public class PartitioningException extends UnprocessableEntityException {
    public PartitioningException(final String message) {
        super(message);
    }

    public PartitioningException(final String msg, final Exception cause) {
        super(msg, cause);
    }

    public PartitioningException(final String msg, final String problemMessage, final Exception cause) {
        super(msg, problemMessage, cause);
    }
}
