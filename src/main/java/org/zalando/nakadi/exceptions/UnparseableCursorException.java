package org.zalando.nakadi.exceptions;

import javax.ws.rs.core.Response;

public class UnparseableCursorException extends NakadiException {
    private final String cursors;

    public UnparseableCursorException(final String msg, final Exception cause, final String cursors) {
        super(msg, cause);
        this.cursors = cursors;
    }

    public String getCursors() {
        return cursors;
    }

    @Override
    protected Response.StatusType getStatus() {
        return Response.Status.BAD_REQUEST;
    }
}
