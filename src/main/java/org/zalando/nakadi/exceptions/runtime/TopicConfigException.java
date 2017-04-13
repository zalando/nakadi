package org.zalando.nakadi.exceptions.runtime;

public class TopicConfigException extends MyNakadiRuntimeException1 {

    public TopicConfigException(final String message, final Exception e) {
        super(message, e);
    }

}
