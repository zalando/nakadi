package org.zalando.nakadi.exceptions.runtime;

public class TopicConfigException extends NakadiBaseException {

    public TopicConfigException(final String message, final Exception e) {
        super(message, e);
    }

    public TopicConfigException(final String message) {
        super(message);
    }

}
