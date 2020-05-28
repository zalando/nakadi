package org.zalando.nakadi.exception;

import org.zalando.nakadi.exceptions.runtime.NakadiBaseException;

public class SchemaValidationException extends NakadiBaseException {
    public SchemaValidationException(final String message) {
        super(message);
    }
}
