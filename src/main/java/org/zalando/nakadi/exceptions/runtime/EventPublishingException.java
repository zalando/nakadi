package org.zalando.nakadi.exceptions.runtime;

public class EventPublishingException extends MyNakadiRuntimeException1 {

    public EventPublishingException(final String msg, final Exception cause) {
        super(msg, cause);
    }

    public EventPublishingException(final String msg) {
        super(msg);
    }
}
