package org.zalando.nakadi.exceptions;

import javax.ws.rs.core.Response;

public class DuplicatedStorageIdException extends NakadiException {
    public DuplicatedStorageIdException(final String message) {
        super(message);
    }

    public DuplicatedStorageIdException(final String msg, final Exception cause) {
        super(msg, cause);
    }

    public DuplicatedStorageIdException(final String msg, final String problemMessage, final Exception cause) {
        super(msg, problemMessage, cause);
    }

    @Override
    protected Response.StatusType getStatus() {
        return Response.Status.CONFLICT;
    }
}
