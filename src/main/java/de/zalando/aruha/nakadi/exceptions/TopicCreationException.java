package de.zalando.aruha.nakadi.exceptions;

import javax.ws.rs.core.Response;

public class TopicCreationException extends NakadiException {
    public TopicCreationException(String msg, Exception cause) {
        super(msg, cause);
    }

    @Override
    protected Response.StatusType getStatus() {
        return Response.Status.SERVICE_UNAVAILABLE;
    }
}
