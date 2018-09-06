package org.zalando.nakadi.exceptions.runtime;

public class NoSuchSubscriptionException extends NakadiBaseException {

    public NoSuchSubscriptionException(final String message) {
        super(message);
    }

    public NoSuchSubscriptionException(final String msg, final Exception cause) {
        super(msg, cause);
    }
}
