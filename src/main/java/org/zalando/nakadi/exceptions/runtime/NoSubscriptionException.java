package org.zalando.nakadi.exceptions.runtime;

public class NoSubscriptionException extends MyNakadiRuntimeException1 {

    public NoSubscriptionException(final String message, final Throwable cause) {
        super(message, cause);
    }
}
