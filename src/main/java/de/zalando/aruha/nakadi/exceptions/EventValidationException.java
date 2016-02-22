package de.zalando.aruha.nakadi.exceptions;

import de.zalando.aruha.nakadi.validation.ValidationError;
import org.zalando.problem.MoreStatus;

import javax.ws.rs.core.Response;

public class EventValidationException extends NakadiException {
    private final ValidationError validationError;

    public EventValidationException(final ValidationError validationError) {
        super(validationError.getMessage());
        this.validationError = validationError;
    }

    @Override
    protected Response.StatusType getStatus() {
        return MoreStatus.UNPROCESSABLE_ENTITY;
    }

    public ValidationError getValidationError() {
        return validationError;
    }
}
