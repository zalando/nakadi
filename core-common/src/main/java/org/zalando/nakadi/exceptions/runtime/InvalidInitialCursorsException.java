package org.zalando.nakadi.exceptions.runtime;

public class InvalidInitialCursorsException extends NakadiBaseException {

    public InvalidInitialCursorsException(final String msg) {
        super(msg);
    }

    public InvalidInitialCursorsException(final String msg, final Exception cause) {
        super(msg, cause);
    }

}
