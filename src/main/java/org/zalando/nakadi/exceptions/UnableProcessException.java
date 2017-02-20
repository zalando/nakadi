package org.zalando.nakadi.exceptions;

public class UnableProcessException extends RuntimeException {

    public UnableProcessException(final String message) {
        super(message);
    }
}
