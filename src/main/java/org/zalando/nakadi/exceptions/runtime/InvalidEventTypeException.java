package org.zalando.nakadi.exceptions.runtime;

public class InvalidEventTypeException extends NakadiRuntimeBaseException {

    public InvalidEventTypeException(final String message) {
        super(message);
    }
}
