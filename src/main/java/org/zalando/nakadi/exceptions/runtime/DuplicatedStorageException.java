package org.zalando.nakadi.exceptions.runtime;

public class DuplicatedStorageException extends NakadiRuntimeBaseException {

    public DuplicatedStorageException(final String msg, final Exception cause) {
        super(msg, cause);
    }

}
