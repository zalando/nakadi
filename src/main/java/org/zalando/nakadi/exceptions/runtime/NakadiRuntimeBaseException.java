package org.zalando.nakadi.exceptions.runtime;

/**
 * Parent class for Nakadi runtime exceptions
 * Name NakadiRuntimeException was already taken for some kind of wrapper. This name is a nice alternative ;)
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
