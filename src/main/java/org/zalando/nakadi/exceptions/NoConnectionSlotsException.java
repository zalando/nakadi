package org.zalando.nakadi.exceptions;

import org.zalando.problem.MoreStatus;

import javax.ws.rs.core.Response;

public class NoConnectionSlotsException extends NakadiException {

    public NoConnectionSlotsException(final String message) {
        super(message);
    }

    @Override
    protected Response.StatusType getStatus() {
        return MoreStatus.TOO_MANY_REQUESTS;
    }

}
