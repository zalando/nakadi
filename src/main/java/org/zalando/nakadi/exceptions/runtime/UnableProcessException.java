package org.zalando.nakadi.exceptions.runtime;

public class UnableProcessException extends NakadiRuntimeBaseException {

    public UnableProcessException(final String message) {
        super(message);
    }

    public UnableProcessException(final String message, final Throwable cause) {
        super(message, cause);
    }
}
