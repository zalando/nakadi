package org.zalando.nakadi.exceptions.runtime;

public class NoSuchEventTypeException extends NakadiBaseException {
    public NoSuchEventTypeException(final String message) {
        super(message);
    }

    public NoSuchEventTypeException(final String msg, final Exception cause) {
        super(msg, cause);
    }
}
