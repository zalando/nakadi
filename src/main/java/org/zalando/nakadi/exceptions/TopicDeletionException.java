package org.zalando.nakadi.exceptions;

import javax.ws.rs.core.Response;

public class TopicDeletionException extends NakadiException {

    public TopicDeletionException(final String msg, final Exception cause) {
        super(msg, cause);
    }

    @Override
    protected Response.StatusType getStatus() {
        return Response.Status.SERVICE_UNAVAILABLE;
    }
}
