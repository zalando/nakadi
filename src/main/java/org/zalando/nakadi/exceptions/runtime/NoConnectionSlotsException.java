package org.zalando.nakadi.exceptions.runtime;

public class NoConnectionSlotsException extends NakadiRuntimeBaseException {

    public NoConnectionSlotsException(final String msg, final Exception cause) {
        super(msg, cause);
    }

    public NoConnectionSlotsException(final String msg) {
        super(msg);
    }
}
