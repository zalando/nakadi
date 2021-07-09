package org.zalando.nakadi.exceptions.runtime;

public class WrongOwningApplicationException extends NakadiBaseException {

    public WrongOwningApplicationException(final String msg) {
        super(msg);
    }

    public WrongOwningApplicationException(final String msg, final Exception cause) {
        super(msg, cause);
    }
}
