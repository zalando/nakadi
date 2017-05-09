package org.zalando.nakadi.exceptions.runtime;

public class DuplicatedStorageException extends MyNakadiRuntimeException1 {

    public DuplicatedStorageException(final String msg, final Exception cause) {
        super(msg, cause);
    }

}
