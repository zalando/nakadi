package org.zalando.nakadi.exceptions.runtime;

public class DuplicatedSubscriptionException extends NakadiBaseException {

    public DuplicatedSubscriptionException(final String msg, final Exception cause) {
        super(msg, cause);
    }

}
