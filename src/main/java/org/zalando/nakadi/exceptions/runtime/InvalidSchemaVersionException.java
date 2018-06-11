package org.zalando.nakadi.exceptions.runtime;

public class InvalidSchemaVersionException extends NakadiRuntimeBaseException {

    public InvalidSchemaVersionException(final String message) {
        super(message);
    }
}
