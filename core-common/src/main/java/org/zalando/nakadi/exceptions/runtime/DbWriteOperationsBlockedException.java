package org.zalando.nakadi.exceptions.runtime;

public class DbWriteOperationsBlockedException extends NakadiBaseException {
    public DbWriteOperationsBlockedException(final String message) {
        super(message);
    }
}
