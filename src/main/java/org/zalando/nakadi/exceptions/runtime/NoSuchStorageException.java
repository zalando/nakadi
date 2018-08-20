package org.zalando.nakadi.exceptions.runtime;

public class NoSuchStorageException extends NakadiRuntimeBaseException {

    public NoSuchStorageException(final String msg) {
        super(msg);
    }

}
