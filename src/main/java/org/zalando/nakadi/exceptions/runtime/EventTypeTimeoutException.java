package org.zalando.nakadi.exceptions.runtime;

public class EventTypeTimeoutException extends NakadiRuntimeBaseException {

    public EventTypeTimeoutException(final String message) {
        super(message);
    }
}
