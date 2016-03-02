package de.zalando.aruha.nakadi.exceptions;

public class InvalidPartitioningKeyFieldsException extends InternalNakadiException {
    public InvalidPartitioningKeyFieldsException(final String message) {
        super(message);
    }

    public InvalidPartitioningKeyFieldsException(final String msg, final Exception cause) {
        super(msg, cause);
    }

    public InvalidPartitioningKeyFieldsException(final String msg, final String problemMessage, final Exception cause) {
        super(msg, problemMessage, cause);
    }
}
