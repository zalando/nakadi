package org.zalando.nakadi.exceptions;

import org.zalando.problem.MoreStatus;

import javax.ws.rs.core.Response;

public class WrongInitialCursorsException extends NakadiException {

    public WrongInitialCursorsException(final String msg) {
        super(msg);
    }

    public WrongInitialCursorsException(final String msg, final Exception cause) {
        super(msg, cause);
    }

    @Override
    protected Response.StatusType getStatus() {
        return MoreStatus.UNPROCESSABLE_ENTITY;
    }
}
