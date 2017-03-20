package org.zalando.nakadi.exceptions;

public class EventTypeDeletionException extends RuntimeException {

    public EventTypeDeletionException(final String message) {
        super(message);
    }

    public EventTypeDeletionException(final String message, final Throwable cause) {
        super(message, cause);
    }
}
