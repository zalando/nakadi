package org.zalando.nakadi.exceptions.runtime;

public class TimelineException extends NakadiBaseException {

    public TimelineException(final String message) {
        super(message);
    }

    public TimelineException(final String message, final Throwable cause) {
        super(message, cause);
    }
}
