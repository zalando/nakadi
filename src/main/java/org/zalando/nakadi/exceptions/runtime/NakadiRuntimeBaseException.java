package org.zalando.nakadi.exceptions.runtime;

/**
 * Parent class for Nakadi runtime exceptions
 */
public class NakadiRuntimeBaseException extends RuntimeException {

    public NakadiRuntimeBaseException() {
    }

    public NakadiRuntimeBaseException(final String message) {
        super(message);
    }

    public NakadiRuntimeBaseException(final String message, final Throwable cause) {
        super(message, cause);
    }

}
