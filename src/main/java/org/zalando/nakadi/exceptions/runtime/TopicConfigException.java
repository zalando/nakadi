package org.zalando.nakadi.exceptions.runtime;

public class TopicConfigException extends NakadiRuntimeBaseException {

    public TopicConfigException(final String message, final Exception e) {
        super(message, e);
    }

}
