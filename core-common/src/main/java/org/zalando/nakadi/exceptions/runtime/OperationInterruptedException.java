package org.zalando.nakadi.exceptions.runtime;

public class OperationInterruptedException extends NakadiBaseException {

    public OperationInterruptedException(final String message, final Throwable cause) {
        super(message, cause);
    }

}
