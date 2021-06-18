package org.zalando.nakadi.exceptions.runtime;

public class EventTypeUnavailableException extends NakadiBaseException {


    public EventTypeUnavailableException(final String message) {
        super(message);
    }

    public EventTypeUnavailableException(final String message, final Throwable cause) {
        super(message, cause);
    }
}
