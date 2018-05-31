package org.zalando.nakadi.exceptions.runtime;

public class InternalNakadiException extends MyNakadiRuntimeException1 {
    public InternalNakadiException(final String message) {
        super(message);
    }

    public InternalNakadiException(final String msg, final Exception cause) {
        super(msg, cause);
    }
}
