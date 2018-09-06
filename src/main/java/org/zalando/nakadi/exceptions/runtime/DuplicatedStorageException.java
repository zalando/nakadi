package org.zalando.nakadi.exceptions.runtime;

public class DuplicatedStorageException extends NakadiBaseException {

    public DuplicatedStorageException(final String msg, final Exception cause) {
        super(msg, cause);
    }

}
