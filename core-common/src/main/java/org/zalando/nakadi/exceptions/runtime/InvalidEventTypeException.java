package org.zalando.nakadi.exceptions.runtime;

public class InvalidEventTypeException extends NakadiBaseException {

    public InvalidEventTypeException(final String message) {
        super(message);
    }

    public InvalidEventTypeException(final Throwable cause) {
        super(cause.getMessage(), cause);
    }
}
