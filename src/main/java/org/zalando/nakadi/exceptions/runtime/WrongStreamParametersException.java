package org.zalando.nakadi.exceptions.runtime;

public class WrongStreamParametersException extends NakadiRuntimeBaseException {

    public WrongStreamParametersException(final String message) {
        super(message);
    }
}
