package org.zalando.nakadi.exceptions.runtime;

public class OperationTimeoutException extends NakadiBaseException {

    public OperationTimeoutException(final String message) {
        super(message);
    }

}
