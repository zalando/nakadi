package org.zalando.nakadi.exceptions.runtime;

public class UnknownOperationException extends NakadiRuntimeBaseException {

    public UnknownOperationException(final String message) {
        super(message);
    }
}
