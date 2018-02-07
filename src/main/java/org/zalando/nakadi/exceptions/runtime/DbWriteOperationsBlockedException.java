package org.zalando.nakadi.exceptions.runtime;

public class DbWriteOperationsBlockedException extends MyNakadiRuntimeException1 {
    public DbWriteOperationsBlockedException(final String message) {
        super(message);
    }
}
