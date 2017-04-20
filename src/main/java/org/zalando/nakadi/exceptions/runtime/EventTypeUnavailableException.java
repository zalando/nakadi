package org.zalando.nakadi.exceptions.runtime;

public class EventTypeUnavailableException extends MyNakadiRuntimeException1 {


    public EventTypeUnavailableException(final String message) {
        super(message);
    }

    public EventTypeUnavailableException(final String message, final Throwable cause) {
        super(message, cause);
    }
}
