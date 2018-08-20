package org.zalando.nakadi.exceptions.runtime;

public class UnprocessableSubscriptionException extends NakadiRuntimeBaseException {
    public UnprocessableSubscriptionException(String message) {
        super(message);
    }
}
