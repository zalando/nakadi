package org.zalando.nakadi.exceptions.runtime;

public class UnprocessableSubscriptionException extends NakadiBaseException {
    public UnprocessableSubscriptionException(String message) {
        super(message);
    }
}
