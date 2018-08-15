package org.zalando.nakadi.exceptions.runtime;

public class LimitReachedException extends NakadiRuntimeBaseException {

    public LimitReachedException(final String msg, final Throwable cause) {
        super(msg, cause);
    }

}
