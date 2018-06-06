package org.zalando.nakadi.exceptions.runtime;

public class DbWriteOperationsBlockedException extends NakadiRuntimeBaseException {
    public DbWriteOperationsBlockedException(final String message) {
        super(message);
    }
}
