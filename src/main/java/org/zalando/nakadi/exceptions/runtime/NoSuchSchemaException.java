package org.zalando.nakadi.exceptions.runtime;

public class NoSuchSchemaException extends NakadiBaseException {

    public NoSuchSchemaException(final String message) {
        super(message);
    }

    public NoSuchSchemaException(final String msg, final Exception cause) {
        super(msg, cause);
    }

}
