package org.zalando.nakadi.exceptions.runtime;

public class UnknownStorageTypeException extends NakadiRuntimeBaseException {

    public UnknownStorageTypeException(final String message) {
        super(message);
    }

    public UnknownStorageTypeException(final String message, final Throwable cause) {
        super(message, cause);
    }
}
