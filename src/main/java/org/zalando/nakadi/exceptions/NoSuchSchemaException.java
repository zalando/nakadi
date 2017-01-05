package org.zalando.nakadi.exceptions;

import javax.ws.rs.core.Response;

public class NoSuchSchemaException extends NakadiException {

    public NoSuchSchemaException(final String message) {
        super(message);
    }

    public NoSuchSchemaException(final String msg, final Exception cause) {
        super(msg, cause);
    }

    @Override
    protected Response.StatusType getStatus() {
        return Response.Status.NOT_FOUND;
    }
}
