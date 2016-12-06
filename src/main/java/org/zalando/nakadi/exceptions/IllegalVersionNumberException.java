package org.zalando.nakadi.exceptions;

import javax.ws.rs.core.Response;

public class IllegalVersionNumberException extends NakadiException {

    public IllegalVersionNumberException(final String message) {
        super(message);
    }

    public IllegalVersionNumberException(final String msg, final Exception cause) {
        super(msg, cause);
    }

    @Override
    protected Response.StatusType getStatus() {
        return Response.Status.NOT_FOUND;
    }
}
