package de.zalando.aruha.nakadi.exceptions;

import de.zalando.aruha.nakadi.domain.Cursor;
import de.zalando.aruha.nakadi.domain.CursorError;

public class InvalidCursorException extends Exception {
    private final CursorError error;
    private final Cursor cursor;

    public InvalidCursorException(final CursorError error, final Cursor cursor) {
        super();
        this.error = error;
        this.cursor = cursor;
    }

    public CursorError getError() {
        return error;
    }

    public Cursor getCursor() {
        return cursor;
    }
}
