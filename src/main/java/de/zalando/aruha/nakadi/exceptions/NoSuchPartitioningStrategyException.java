package de.zalando.aruha.nakadi.exceptions;

public class NoSuchPartitioningStrategyException extends UnprocessableEntityException {

    public NoSuchPartitioningStrategyException(String message) {
        super(message);
    }

}
