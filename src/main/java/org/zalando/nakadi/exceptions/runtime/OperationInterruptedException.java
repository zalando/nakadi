package org.zalando.nakadi.exceptions.runtime;

public class OperationInterruptedException extends NakadiRuntimeBaseException {

    public OperationInterruptedException(final String message, final Throwable cause) {
        super(message, cause);
    }

}
