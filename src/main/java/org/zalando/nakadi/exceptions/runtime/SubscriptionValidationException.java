package org.zalando.nakadi.exceptions.runtime;

public class SubscriptionValidationException extends NakadiBaseException {

    public SubscriptionValidationException(final String message) {
        super(message);
    }

}