package org.zalando.nakadi.exceptions;

import org.zalando.problem.MoreStatus;

import javax.ws.rs.core.Response;

public class InvalidStreamIdException extends NakadiException {

    public InvalidStreamIdException(final String message) {
        super(message);
    }

    @Override
    protected Response.StatusType getStatus() {
        return MoreStatus.UNPROCESSABLE_ENTITY;
    }
}
