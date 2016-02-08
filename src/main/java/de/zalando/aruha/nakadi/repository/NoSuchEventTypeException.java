package de.zalando.aruha.nakadi.repository;

import de.zalando.aruha.nakadi.NakadiException;

public class NoSuchEventTypeException extends NakadiException {
    public NoSuchEventTypeException(final String message) {
        super(message);
    }

    public NoSuchEventTypeException(String msg, Exception cause) {
        super(msg, cause);
    }
}
