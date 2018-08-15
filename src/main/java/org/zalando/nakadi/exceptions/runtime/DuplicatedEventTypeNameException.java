package org.zalando.nakadi.exceptions.runtime;

public class DuplicatedEventTypeNameException extends NakadiRuntimeBaseException {

    public DuplicatedEventTypeNameException(final String msg, final Exception cause) {
        super(msg, cause);
    }

    public DuplicatedEventTypeNameException(final String msg) {
        super(msg);
    }
}
