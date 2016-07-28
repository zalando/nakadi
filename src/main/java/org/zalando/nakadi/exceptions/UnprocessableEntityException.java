package org.zalando.nakadi.exceptions;

import org.zalando.problem.MoreStatus;

import javax.ws.rs.core.Response;

public class UnprocessableEntityException extends NakadiException {
    public UnprocessableEntityException(final String message) {
        super(message);
    }

    public UnprocessableEntityException(final String msg, final Exception cause) {
        super(msg, cause);
    }

    public UnprocessableEntityException(final String msg, final String problemMessage, final Exception cause) {
        super(msg, problemMessage, cause);
    }

    @Override
    protected Response.StatusType getStatus() {
        return MoreStatus.UNPROCESSABLE_ENTITY;
    }
}
