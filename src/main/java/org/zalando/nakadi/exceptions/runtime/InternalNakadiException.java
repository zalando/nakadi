package org.zalando.nakadi.exceptions.runtime;

public class InternalNakadiException extends NakadiBaseException {
    public InternalNakadiException(final String message) {
        super(message);
    }

    public InternalNakadiException(final String msg, final Exception cause) {
        super(msg, cause);
    }
}
