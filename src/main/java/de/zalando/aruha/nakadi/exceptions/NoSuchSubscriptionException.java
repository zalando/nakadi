package de.zalando.aruha.nakadi.exceptions;

import javax.ws.rs.core.Response;

public class NoSuchSubscriptionException extends NakadiException {
    public NoSuchSubscriptionException(final String message) {
        super(message);
    }

    public NoSuchSubscriptionException(String msg, Exception cause) {
        super(msg, cause);
    }

    @Override
    protected Response.StatusType getStatus() {
        return Response.Status.NOT_FOUND;
    }
}
