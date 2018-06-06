package org.zalando.nakadi.exceptions.runtime;

public class NoSuchSubscriptionException extends NakadiRuntimeBaseException {

    public NoSuchSubscriptionException(final String message) {
        super(message);
    }

}
