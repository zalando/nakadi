package org.zalando.nakadi.exceptions.runtime;

public class CannotAddParitionToTopicException extends NakadiBaseException {
    public CannotAddParitionToTopicException(final String msg, final Exception cause) {
        super(msg, cause);
    }
}
