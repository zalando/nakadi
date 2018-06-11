package org.zalando.nakadi.exceptions.runtime;

public class EventTypeOptionsValidationException extends NakadiRuntimeBaseException {

    public EventTypeOptionsValidationException(final String message) {
        super(message);
    }
}
