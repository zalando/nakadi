package org.zalando.nakadi.exceptions.runtime;

public class NoSuchPartitionStrategyException extends NakadiRuntimeBaseException {

    public NoSuchPartitionStrategyException(final String message) {
        super(message);
    }

}
