package org.zalando.nakadi.exceptions.runtime;

public class UnknownOperationException extends NakadiBaseException {

    public UnknownOperationException(final String message) {
        super(message);
    }
}
