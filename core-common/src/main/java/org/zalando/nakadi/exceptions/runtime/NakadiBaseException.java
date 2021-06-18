package org.zalando.nakadi.exceptions.runtime;

/**
 * Parent class for Nakadi runtime exceptions
 * Name NakadiRuntimeException was already taken for some kind of wrapper. This name is a nice alternative ;)
 */
public class NakadiBaseException extends RuntimeException {

    public NakadiBaseException() {
    }

    public NakadiBaseException(final String message) {
        super(message);
    }

    public NakadiBaseException(final String message, final Throwable cause) {
        super(message, cause);
    }

}
