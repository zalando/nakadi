package org.zalando.nakadi.exceptions.runtime;

public class OperationTimeoutException extends MyNakadiRuntimeException1 {

    public OperationTimeoutException(final String message, final Throwable cause) {
        super(message, cause);
    }

}
