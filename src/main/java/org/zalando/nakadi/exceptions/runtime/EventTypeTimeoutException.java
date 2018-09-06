package org.zalando.nakadi.exceptions.runtime;

public class EventTypeTimeoutException extends NakadiBaseException {

    public EventTypeTimeoutException(final String message) {
        super(message);
    }
}
