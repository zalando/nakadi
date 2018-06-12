package org.zalando.nakadi.exceptions.runtime;

public class TopicDeletionException extends MyNakadiRuntimeException1 {

    public TopicDeletionException(final String msg, final Exception cause) {
        super(msg, cause);
    }
}
