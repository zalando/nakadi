package org.zalando.nakadi.exceptions.runtime;

public class InvalidStorageTypeException extends NakadiRuntimeBaseException {

    public InvalidStorageTypeException(final String message) {
        super(message);
    }
}
