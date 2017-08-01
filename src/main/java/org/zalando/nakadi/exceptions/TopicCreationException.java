package org.zalando.nakadi.exceptions;

import javax.ws.rs.core.Response;

public class TopicCreationException extends NakadiException {

    public TopicCreationException(final String msg) {
        super(msg);
    }

    public TopicCreationException(final String msg, final Exception cause) {
        super(msg, cause);
    }

    @Override
    protected Response.StatusType getStatus() {
        return Response.Status.SERVICE_UNAVAILABLE;
    }
}
