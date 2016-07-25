package org.zalando.nakadi.exceptions;

import javax.ws.rs.core.Response;

public class InternalNakadiException extends NakadiException {
    public InternalNakadiException(final String message) {
        super(message);
    }

    public InternalNakadiException(final String msg, final Exception cause) {
        super(msg, cause);
    }

    public InternalNakadiException(final String msg, final String problemMessage, final Exception cause) {
        super(msg, problemMessage, cause);
    }

    @Override
    protected Response.StatusType getStatus() {
        return Response.Status.INTERNAL_SERVER_ERROR;
    }
}
