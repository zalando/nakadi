package org.zalando.nakadi.exceptions.runtime;

public class OperationInterruptedException extends MyNakadiRuntimeException1 {

    public OperationInterruptedException(final String message, final Throwable cause) {
        super(message, cause);
    }

}
