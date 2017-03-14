package org.zalando.nakadi.exceptions.runtime;

public class InconsistentStateException extends MyNakadiRuntimeException1 {

    public InconsistentStateException(final String msg, final Exception cause) {
        super(msg, cause);
    }

    public InconsistentStateException(final String msg) {
        super(msg);
    }

}
