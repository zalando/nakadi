package org.zalando.nakadi.exceptions.runtime;

public class WrongStreamParametersException extends NakadiBaseException {

    public WrongStreamParametersException(final String message) {
        super(message);
    }
}
