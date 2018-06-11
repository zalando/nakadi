package org.zalando.nakadi.exceptions.runtime;

public class CursorsAreEmptyException extends NakadiRuntimeBaseException {
    public CursorsAreEmptyException() {
        super("Cursors are empty");
    }
}
