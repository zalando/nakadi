package de.zalando.aruha.nakadi.repository;

public class NoSuchEventTypeException extends Exception {
    public NoSuchEventTypeException() {
        super();
    }

    public NoSuchEventTypeException(final String message) {
        super(message);
    }

    public NoSuchEventTypeException(final String message, final Throwable cause) {
        super(message, cause);
    }

    public NoSuchEventTypeException(final Throwable cause) {
        super(cause);
    }
}