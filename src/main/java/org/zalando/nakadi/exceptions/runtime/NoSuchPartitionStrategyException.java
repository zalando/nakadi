package org.zalando.nakadi.exceptions.runtime;

public class NoSuchPartitionStrategyException extends UnprocessableEntityException {

    public NoSuchPartitionStrategyException(final String message) {
        super(message);
    }

}
