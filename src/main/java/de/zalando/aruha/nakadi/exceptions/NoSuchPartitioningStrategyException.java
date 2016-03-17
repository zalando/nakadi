package de.zalando.aruha.nakadi.exceptions;

public class NoSuchPartitioningStrategyException extends UnprocessableEntityException {

    public NoSuchPartitioningStrategyException(final String message) {
        super(message);
    }

}
