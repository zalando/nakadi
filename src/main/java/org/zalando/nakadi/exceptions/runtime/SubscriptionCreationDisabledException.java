package org.zalando.nakadi.exceptions.runtime;

public class SubscriptionCreationDisabledException extends NakadiBaseException {

    public SubscriptionCreationDisabledException(final String message) {
        super(message);
    }
}
