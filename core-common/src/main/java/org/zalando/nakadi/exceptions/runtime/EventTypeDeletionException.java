package org.zalando.nakadi.exceptions.runtime;

public class EventTypeDeletionException extends NakadiBaseException {

    public EventTypeDeletionException(final String message) {
        super(message);
    }

    public EventTypeDeletionException(final String message, final Throwable cause) {
        super(message, cause);
    }
}
