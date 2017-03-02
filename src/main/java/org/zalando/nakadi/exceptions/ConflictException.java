package org.zalando.nakadi.exceptions;

public class ConflictException extends RuntimeException {

    public ConflictException(String message, Throwable cause) {
        super(message, cause);
    }
}
