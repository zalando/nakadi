package org.zalando.nakadi.exceptions.runtime;

import org.springframework.validation.Errors;

public class ValidationException extends NakadiBaseException {

    private final Errors errors;

    public ValidationException(final Errors errors) {
        super();
        this.errors = errors;
    }

    public Errors getErrors() {
        return errors;
    }
}
