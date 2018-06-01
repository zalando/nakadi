package org.zalando.nakadi.exceptions.runtime;

public class InvalidSchemaVersionException extends MyNakadiRuntimeException1 {

    public InvalidSchemaVersionException(final String message) {
        super(message);
    }
}
