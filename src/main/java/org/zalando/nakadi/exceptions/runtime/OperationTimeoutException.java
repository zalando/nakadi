package org.zalando.nakadi.exceptions.runtime;

public class OperationTimeoutException extends MyNakadiRuntimeException1 {

    public OperationTimeoutException(final String message) {
        super(message);
    }

}
