package org.zalando.nakadi.exceptions;

import javax.ws.rs.core.Response;

public class NoSuchSubscriptionException extends NakadiException {

    public NoSuchSubscriptionException(final String message) {
        super(message);
    }

    public NoSuchSubscriptionException(final String msg, final Exception cause) {
        super(msg, cause);
    }

    @Override
    protected Response.StatusType getStatus() {
        return Response.Status.NOT_FOUND;
    }
}
