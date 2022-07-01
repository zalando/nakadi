package org.zalando.nakadi.exceptions.runtime;

public class UnsupportedSchemaTypeException extends NakadiBaseException {
    public UnsupportedSchemaTypeException(final String message) {
        super(message);
    }
}