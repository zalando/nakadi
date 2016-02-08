package de.zalando.aruha.nakadi.repository;

public class DuplicatedEventTypeNameException extends Exception {
    private final Exception exception;
    private final String name;

    public DuplicatedEventTypeNameException(Exception e, String name) {
        this.exception = e;
        this.name = name;
    }

    public Exception getException() { return exception; }

    public String getName() { return name; }
}
