package org.zalando.nakadi.exceptions.runtime;

public class StorageIsUsedException extends NakadiBaseException {

    public StorageIsUsedException(final String msg, final Exception cause) {
        super(msg, cause);
    }

}
