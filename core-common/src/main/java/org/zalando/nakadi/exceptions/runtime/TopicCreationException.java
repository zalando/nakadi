package org.zalando.nakadi.exceptions.runtime;

public class TopicCreationException extends NakadiBaseException {

    public TopicCreationException(final String msg) {
        super(msg);
    }

    public TopicCreationException(final String msg, final Exception cause) {
        super(msg, cause);
    }
}
