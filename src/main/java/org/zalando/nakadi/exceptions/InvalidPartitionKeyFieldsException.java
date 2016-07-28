package org.zalando.nakadi.exceptions;

public class InvalidPartitionKeyFieldsException extends PartitioningException {
    public InvalidPartitionKeyFieldsException(final String message) {
        super(message);
    }

    public InvalidPartitionKeyFieldsException(final String msg, final Exception cause) {
        super(msg, cause);
    }

    public InvalidPartitionKeyFieldsException(final String msg, final String problemMessage, final Exception cause) {
        super(msg, problemMessage, cause);
    }
}
