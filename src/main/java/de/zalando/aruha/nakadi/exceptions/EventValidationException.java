package de.zalando.aruha.nakadi.exceptions;

import de.zalando.aruha.nakadi.validation.ValidationError;
import org.zalando.problem.MoreStatus;

import javax.ws.rs.core.Response;

public class EventValidationException extends NakadiException {
    public EventValidationException(final String message) {
        super(message);
    }

    public EventValidationException(final ValidationError validationError) {
        super(validationError.getMessage());
    }

    @Override
    protected Response.StatusType getStatus() {
        return MoreStatus.UNPROCESSABLE_ENTITY;
    }
}
