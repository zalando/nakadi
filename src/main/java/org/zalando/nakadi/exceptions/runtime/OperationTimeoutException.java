package org.zalando.nakadi.exceptions.runtime;

public class OperationTimeoutException extends NakadiRuntimeBaseException {

    public OperationTimeoutException(final String message) {
        super(message);
    }

}
