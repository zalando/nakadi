package org.zalando.nakadi.exceptions.runtime;

public class TopicRepositoryException extends MyNakadiRuntimeException1 {

    public TopicRepositoryException(final String message) {
        super(message);
    }

    public TopicRepositoryException(final String message, final Throwable cause) {
        super(message, cause);
    }
}
