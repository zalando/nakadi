package org.zalando.nakadi.exceptions.runtime;

public class EventValidationException extends NakadiBaseException {

    public EventValidationException(final String msg, final Exception cause) {
        super(msg, cause);
    }

    public EventValidationException(final String msg) {
        super(msg);
    }
}
