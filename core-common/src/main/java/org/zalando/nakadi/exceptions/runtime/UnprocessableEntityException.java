package org.zalando.nakadi.exceptions.runtime;

public class UnprocessableEntityException extends NakadiBaseException {

    public UnprocessableEntityException(final String message) {
        super(message);
    }

    public UnprocessableEntityException(final String message, final Throwable exception) {
        super(message, exception);
    }
}
