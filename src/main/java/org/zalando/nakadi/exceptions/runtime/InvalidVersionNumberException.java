package org.zalando.nakadi.exceptions.runtime;

public class InvalidVersionNumberException extends NakadiBaseException {

    public InvalidVersionNumberException(final String message) {
        super(message);
    }
}
