package org.zalando.nakadi.exceptions;

public class CompactionException extends UnprocessableEntityException {

    public CompactionException(final String message) {
        super(message);
    }

}
