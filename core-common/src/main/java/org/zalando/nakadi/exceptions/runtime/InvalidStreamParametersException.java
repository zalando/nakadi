package org.zalando.nakadi.exceptions.runtime;

public class InvalidStreamParametersException extends NakadiBaseException {

    public InvalidStreamParametersException(final String message) {
        super(message);
    }
}
