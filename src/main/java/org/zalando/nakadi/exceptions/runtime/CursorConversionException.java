package org.zalando.nakadi.exceptions.runtime;

public class CursorConversionException extends NakadiRuntimeBaseException {
    public CursorConversionException(final String message, final Exception e) {
        super(message, e);
    }
}
