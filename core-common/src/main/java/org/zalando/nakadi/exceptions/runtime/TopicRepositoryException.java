package org.zalando.nakadi.exceptions.runtime;

public class TopicRepositoryException extends NakadiBaseException {

    public TopicRepositoryException(final String message) {
        super(message);
    }

    public TopicRepositoryException(final String message, final Throwable cause) {
        super(message, cause);
    }
}
