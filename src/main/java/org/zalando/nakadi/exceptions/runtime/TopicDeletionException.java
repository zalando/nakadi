package org.zalando.nakadi.exceptions.runtime;

public class TopicDeletionException extends NakadiRuntimeBaseException {

    public TopicDeletionException(final String msg, final Exception cause) {
        super(msg, cause);
    }
}
