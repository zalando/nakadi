package org.zalando.nakadi.exceptions.runtime;

public class NoSuchPartitionStrategyException extends NakadiBaseException {

    public NoSuchPartitionStrategyException(final String message) {
        super(message);
    }

}
