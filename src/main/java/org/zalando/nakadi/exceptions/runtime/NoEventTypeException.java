package org.zalando.nakadi.exceptions.runtime;

public class NoEventTypeException extends MyNakadiRuntimeException1 {

    public NoEventTypeException(final String msg) {
        super(msg);
    }

    public NoEventTypeException(final String message, final NoSuchEventTypeException e) {
        super(message, e);
    }
}
