package org.zalando.nakadi.exceptions;

import javax.ws.rs.core.Response;

public class EventPublishingTimeoutException extends NakadiException {

    public EventPublishingTimeoutException(final String message, final Exception e) {
        super(message, e);
    }

    public EventPublishingTimeoutException(final String message) {
        super(message);
    }

    @Override
    protected Response.StatusType getStatus() {
        return Response.Status.SERVICE_UNAVAILABLE;
    }
}
