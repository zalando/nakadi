package org.zalando.nakadi.exceptions.runtime;

public class CannotAddPartitionToTopicException extends NakadiBaseException {
    public CannotAddPartitionToTopicException(final String msg, final Exception cause) {
        super(msg, cause);
    }
}
