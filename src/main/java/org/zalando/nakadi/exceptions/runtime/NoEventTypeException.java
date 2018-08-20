package org.zalando.nakadi.exceptions.runtime;

public class NoEventTypeException extends NakadiRuntimeBaseException {

    public NoEventTypeException(final String msg) {
        super(msg);
    }

    public NoEventTypeException(final String message, final NoSuchEventTypeException e) {
        super(message, e);
    }
}
