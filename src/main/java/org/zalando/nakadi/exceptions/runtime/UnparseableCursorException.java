package org.zalando.nakadi.exceptions.runtime;

public class UnparseableCursorException extends NakadiBaseException {
    private final String cursors;

    public UnparseableCursorException(final String msg, final Exception cause, final String cursors) {
        super(msg, cause);
        this.cursors = cursors;
    }

    public String getCursors() {
        return cursors;
    }
}
