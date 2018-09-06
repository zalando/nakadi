package org.zalando.nakadi.exceptions.runtime;

public class CursorsAreEmptyException extends NakadiBaseException {
    public CursorsAreEmptyException() {
        super("Cursors are empty");
    }
}
