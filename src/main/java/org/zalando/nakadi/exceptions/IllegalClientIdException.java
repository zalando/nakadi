package org.zalando.nakadi.exceptions;

public class IllegalClientIdException extends RuntimeException {

    public IllegalClientIdException(String message) {
        super(message);
    }

}
