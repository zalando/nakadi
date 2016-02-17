package de.zalando.aruha.nakadi.repository;

import de.zalando.aruha.nakadi.exceptions.NakadiException;

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
