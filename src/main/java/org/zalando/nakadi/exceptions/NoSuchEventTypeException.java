package org.zalando.nakadi.exceptions;

import javax.ws.rs.core.Response;

public class NoSuchEventTypeException extends NakadiException {
    public NoSuchEventTypeException(final String message) {
        super(message);
    }

    public NoSuchEventTypeException(final String msg, final Exception cause) {
        super(msg, cause);
    }

    @Override
    protected Response.StatusType getStatus() {
        return Response.Status.NOT_FOUND;
    }
}
