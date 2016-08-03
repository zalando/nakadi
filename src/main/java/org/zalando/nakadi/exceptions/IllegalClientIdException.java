package org.zalando.nakadi.exceptions;

public class IllegalClientIdException extends RuntimeException {

    public IllegalClientIdException(final String message) {
        super(message);
    }

}
