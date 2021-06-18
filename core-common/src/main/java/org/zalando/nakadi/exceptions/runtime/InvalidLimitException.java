package org.zalando.nakadi.exceptions.runtime;

public class InvalidLimitException extends NakadiBaseException {

    public InvalidLimitException(final String message) {
        super(message);
    }
}
