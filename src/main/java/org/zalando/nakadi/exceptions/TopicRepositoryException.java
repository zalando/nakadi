package org.zalando.nakadi.exceptions;

public class TopicRepositoryException extends RuntimeException {

    public TopicRepositoryException(final String message) {
        super(message);
    }

    public TopicRepositoryException(final String message, final Throwable cause) {
        super(message, cause);
    }
}
