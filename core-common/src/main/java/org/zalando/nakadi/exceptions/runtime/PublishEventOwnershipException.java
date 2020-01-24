package org.zalando.nakadi.exceptions.runtime;

public class PublishEventOwnershipException extends NakadiBaseException {
    public PublishEventOwnershipException(final String msg, final Exception cause) {
        super(msg, cause);
    }
}
