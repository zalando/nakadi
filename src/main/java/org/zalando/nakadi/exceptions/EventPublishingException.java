package org.zalando.nakadi.exceptions;

import org.zalando.problem.MoreStatus;

import javax.ws.rs.core.Response;

public class EventPublishingException extends NakadiException {
    public EventPublishingException(final String message, final Exception e) {
        super(message, e);
    }

    public EventPublishingException(final String message) {
        super(message);
    }

    @Override
    protected Response.StatusType getStatus() {
        return MoreStatus.MULTI_STATUS;
    }
}
