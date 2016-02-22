package de.zalando.aruha.nakadi.partitioning;

import de.zalando.aruha.nakadi.exceptions.InternalNakadiException;

public class InvalidOrderingKeyFieldsException extends InternalNakadiException {
    public InvalidOrderingKeyFieldsException(final String message) {
        super(message);
    }

    public InvalidOrderingKeyFieldsException(final String msg, final Exception cause) {
        super(msg, cause);
    }

    public InvalidOrderingKeyFieldsException(final String msg, final String problemMessage, final Exception cause) {
        super(msg, problemMessage, cause);
    }
}
