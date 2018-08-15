package org.zalando.nakadi.exceptions.runtime;

public class DuplicatedSubscriptionException extends NakadiRuntimeBaseException {

    public DuplicatedSubscriptionException(final String msg, final Exception cause) {
        super(msg, cause);
    }

}
