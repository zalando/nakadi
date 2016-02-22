package de.zalando.aruha.nakadi.validation;

public class ValidationError {
    final private String message;

    public ValidationError(final String message) {
        this.message = message;
    }

    public String getMessage() {
        return message;
    }
}
