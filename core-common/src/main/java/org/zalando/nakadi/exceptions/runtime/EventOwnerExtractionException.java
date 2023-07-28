package org.zalando.nakadi.exceptions.runtime;

public class EventOwnerExtractionException extends NakadiBaseException {

    public EventOwnerExtractionException(final String msg, final Exception cause) {
        super(msg, cause);
    }

    public EventOwnerExtractionException(final String msg) {
        super(msg);
    }
}
