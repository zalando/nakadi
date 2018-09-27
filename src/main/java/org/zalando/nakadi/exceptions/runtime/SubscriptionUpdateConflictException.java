package org.zalando.nakadi.exceptions.runtime;

public class SubscriptionUpdateConflictException extends NakadiBaseException {
    public SubscriptionUpdateConflictException(final String message) {
        super(message);
    }
}
