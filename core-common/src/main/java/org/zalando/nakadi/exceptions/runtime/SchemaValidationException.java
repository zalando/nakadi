package org.zalando.nakadi.exceptions.runtime;

public class SchemaValidationException extends NakadiBaseException {
    public SchemaValidationException(final String message) {
        super(message);
    }
}
