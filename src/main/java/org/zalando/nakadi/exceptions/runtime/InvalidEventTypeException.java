package org.zalando.nakadi.exceptions.runtime;

public class InvalidEventTypeException extends MyNakadiRuntimeException1 {

    public InvalidEventTypeException(final String message) {
        super(message);
    }
}
