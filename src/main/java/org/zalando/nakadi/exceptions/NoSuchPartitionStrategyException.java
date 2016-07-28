package org.zalando.nakadi.exceptions;

public class NoSuchPartitionStrategyException extends UnprocessableEntityException {

    public NoSuchPartitionStrategyException(final String message) {
        super(message);
    }

}
