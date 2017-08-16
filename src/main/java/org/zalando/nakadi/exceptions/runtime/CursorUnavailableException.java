package org.zalando.nakadi.exceptions.runtime;

public class CursorUnavailableException extends MyNakadiRuntimeException1 {
    public CursorUnavailableException(final String message, final Exception e) {
        super(message, e);
    }
}
