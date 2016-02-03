package de.zalando.aruha.nakadi.repository;

public class DuplicatedEventTypeNameException extends Exception {
    private Exception exception;
    private String name;

    public DuplicatedEventTypeNameException(Exception e, String name) {
        this.exception = e;
        this.name = name;
    }

    public Exception getException() { return exception; }

    public void setException(Exception exception) { this.exception = exception; }

    public String getName() { return name; }

    public void setName(String name) { this.name = name; }
}
