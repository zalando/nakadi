package org.zalando.nakadi.exceptions.runtime;

public class NoSubscriptionException extends NakadiRuntimeBaseException {

    public NoSubscriptionException(final String message, final Throwable cause) {
        super(message, cause);
    }
}
