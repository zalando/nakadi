package org.zalando.nakadi.exceptions.runtime;

public class NoSuchSubscriptionException extends NakadiBaseException {

    public NoSuchSubscriptionException(final String message) {
        super(message);
    }

    public NoSuchSubscriptionException(final String msg, final Exception cause) {
        super(msg, cause);
    }

    public static NoSuchSubscriptionException withSubscriptionId(final String subscriptionId, final Exception cause) {
        return new NoSuchSubscriptionException("Subscription with id \"" + subscriptionId + "\" does not exist", cause);
    }
}
