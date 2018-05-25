package org.zalando.nakadi.exceptions.runtime;

public class LimitReachedException extends MyNakadiRuntimeException1 {

    public LimitReachedException(final String msg, final Throwable cause) {
        super(msg, cause);
    }

}
