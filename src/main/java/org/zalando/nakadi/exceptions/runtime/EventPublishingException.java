package org.zalando.nakadi.exceptions.runtime;

public class EventPublishingException extends NakadiRuntimeBaseException {

    public EventPublishingException(final String msg, final Exception cause) {
        super(msg, cause);
    }

    public EventPublishingException(final String msg) {
        super(msg);
    }
}
