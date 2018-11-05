package org.zalando.nakadi.exceptions.runtime;

public class UnprocessableSubscriptionException extends NakadiBaseException {
    public UnprocessableSubscriptionException(final String message) {
        super(message);
    }
}
