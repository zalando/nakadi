package org.zalando.nakadi.exceptions.runtime;

public class InsufficientAuthorizationException extends MyNakadiRuntimeException1 {

    public InsufficientAuthorizationException(final String message) {
        super(message);
    }

    public InsufficientAuthorizationException(final String message, final Throwable cause) {
        super(message, cause);
    }
}
