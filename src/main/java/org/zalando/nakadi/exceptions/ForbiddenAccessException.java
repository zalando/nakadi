package org.zalando.nakadi.exceptions;

public class ForbiddenAccessException extends RuntimeException {

    public ForbiddenAccessException(final String message) {
        super(message);
    }
}
