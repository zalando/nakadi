package org.zalando.nakadi.exceptions.runtime;

public class UnknownStatusCodeException extends NakadiBaseException {

    public UnknownStatusCodeException(final String message) {
        super(message);
    }
}
