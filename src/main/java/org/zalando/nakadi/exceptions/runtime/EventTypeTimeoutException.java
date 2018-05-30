package org.zalando.nakadi.exceptions.runtime;

public class EventTypeTimeoutException extends MyNakadiRuntimeException1 {

    public EventTypeTimeoutException(final String message) {
        super(message);
    }
}
