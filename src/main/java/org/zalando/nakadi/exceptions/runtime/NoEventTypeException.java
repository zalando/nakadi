package org.zalando.nakadi.exceptions.runtime;

import org.zalando.nakadi.exceptions.NoSuchEventTypeException;

public class NoEventTypeException extends MyNakadiRuntimeException1 {

    public NoEventTypeException(final String msg) {
        super(msg);
    }

    public NoEventTypeException(final String message, final NoSuchEventTypeException e) {
        super(message, e);
    }
}
