package org.zalando.nakadi.exceptions.runtime;

public class TimelineException extends MyNakadiRuntimeException1 {

    public TimelineException(final String message) {
        super(message);
    }

    public TimelineException(final String message, final Throwable cause) {
        super(message, cause);
    }
}
