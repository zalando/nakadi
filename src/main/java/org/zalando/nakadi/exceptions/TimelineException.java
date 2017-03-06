package org.zalando.nakadi.exceptions;

public class TimelineException extends RuntimeException {

    public TimelineException(final String message) {
        super(message);
    }

    public TimelineException(final String message, final Throwable cause) {
        super(message, cause);
    }
}
