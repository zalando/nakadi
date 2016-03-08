package de.zalando.aruha.nakadi.exceptions;

import javax.ws.rs.core.Response;

public class DuplicatedEventTypeNameException extends NakadiException {
    public DuplicatedEventTypeNameException(final String message) {
        super(message);
    }

    public DuplicatedEventTypeNameException(final String msg, final Exception cause) {
        super(msg, cause);
    }

    public DuplicatedEventTypeNameException(final String msg, final String problemMessage, final Exception cause) {
        super(msg, problemMessage, cause);
    }

    @Override
    protected Response.StatusType getStatus() {
        return Response.Status.CONFLICT;
    }
}
