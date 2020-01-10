package org.zalando.nakadi.validation;

public class ValidationError {
    private final String message;

    public ValidationError(final String message) {
        this.message = message;
    }

    public String getMessage() {
        return message;
    }
}
