package org.zalando.nakadi.exceptions.runtime;

public class InvalidPartitionKeyFieldsException extends PartitioningException {
    public InvalidPartitionKeyFieldsException(final String message) {
        super(message);
    }

    public InvalidPartitionKeyFieldsException(final String msg, final Exception cause) {
        super(msg, cause);
    }

}
