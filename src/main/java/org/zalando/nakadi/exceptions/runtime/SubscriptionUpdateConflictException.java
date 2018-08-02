package org.zalando.nakadi.exceptions.runtime;

public class SubscriptionUpdateConflictException extends MyNakadiRuntimeException1 {
    public SubscriptionUpdateConflictException(final String message) {
        super(message);
    }
}
