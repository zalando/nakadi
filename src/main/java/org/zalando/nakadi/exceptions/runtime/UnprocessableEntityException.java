package org.zalando.nakadi.exceptions.runtime;

public class UnprocessableEntityException extends MyNakadiRuntimeException1 {
    public UnprocessableEntityException(final String message) {
        super(message);
    }

    public UnprocessableEntityException(final String msg, final Exception cause) {
        super(msg, cause);
    }

}
