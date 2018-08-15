package org.zalando.nakadi.exceptions.runtime;

public class SubscriptionUpdateConflictException extends NakadiRuntimeBaseException {
    public SubscriptionUpdateConflictException(final String message) {
        super(message);
    }
}
