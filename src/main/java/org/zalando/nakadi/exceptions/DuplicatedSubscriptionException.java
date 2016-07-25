package org.zalando.nakadi.exceptions;

import javax.ws.rs.core.Response;

public class DuplicatedSubscriptionException extends NakadiException {

    public DuplicatedSubscriptionException(final String msg, final Exception cause) {
        super(msg, cause);
    }

    @Override
    protected Response.StatusType getStatus() {
        return Response.Status.CONFLICT;
    }
}
