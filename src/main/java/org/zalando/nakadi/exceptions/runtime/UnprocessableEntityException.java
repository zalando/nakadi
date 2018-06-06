package org.zalando.nakadi.exceptions.runtime;

public class UnprocessableEntityException extends NakadiRuntimeBaseException {
    public UnprocessableEntityException(final String message) {
        super(message);
    }

    public UnprocessableEntityException(final String msg, final Exception cause) {
        super(msg, cause);
    }

}
