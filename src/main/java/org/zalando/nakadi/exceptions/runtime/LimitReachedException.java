package org.zalando.nakadi.exceptions.runtime;

public class LimitReachedException extends NakadiBaseException {

    public LimitReachedException(final String msg, final Throwable cause) {
        super(msg, cause);
    }

}
