package org.zalando.nakadi.exceptions.runtime;

public class DuplicatedEventTypeNameException extends MyNakadiRuntimeException1 {

    public DuplicatedEventTypeNameException(final String msg, final Exception cause) {
        super(msg, cause);
    }

    public DuplicatedEventTypeNameException(final String msg) {
        super(msg);
    }
}
