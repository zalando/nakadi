package org.zalando.nakadi.exceptions;

import org.zalando.nakadi.validation.ValidationError;
import org.zalando.problem.MoreStatus;

import javax.ws.rs.core.Response;

public class EventSizeValidationException extends NakadiException {
    public EventSizeValidationException(final String message) {
        super(message);
    }

    public EventSizeValidationException (final ValidationError validationError) {
        super(validationError.getMessage());
    }

    @Override
    protected Response.StatusType getStatus() {
        return MoreStatus.UNPROCESSABLE_ENTITY;
    }
}
