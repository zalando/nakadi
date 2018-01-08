package org.zalando.nakadi.exceptions.runtime;

public class CursorsAreEmptyException extends MyNakadiRuntimeException1 {
    public CursorsAreEmptyException() {
        super("Cursors are empty");
    }
}
