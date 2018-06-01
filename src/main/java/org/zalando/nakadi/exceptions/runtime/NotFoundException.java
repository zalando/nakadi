package org.zalando.nakadi.exceptions.runtime;

public class NotFoundException extends MyNakadiRuntimeException1 {

    public NotFoundException(final String message) {
        super(message);
    }

    public NotFoundException(final String message, final Throwable cause) {
        super(message, cause);
    }
}
