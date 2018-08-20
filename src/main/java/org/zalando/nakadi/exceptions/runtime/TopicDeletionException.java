package org.zalando.nakadi.exceptions.runtime;

public class TopicDeletionException extends NakadiBaseException {

    public TopicDeletionException(final String msg, final Exception cause) {
        super(msg, cause);
    }
}
