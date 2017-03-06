package org.zalando.nakadi.exceptions.runtime;

public class WrongInitialCursorsException extends MyNakadiRuntimeException1 {

    public WrongInitialCursorsException(final String msg) {
        super(msg);
    }

    public WrongInitialCursorsException(final String msg, final Exception cause) {
        super(msg, cause);
    }

}
