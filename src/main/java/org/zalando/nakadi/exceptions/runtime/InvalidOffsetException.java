package org.zalando.nakadi.exceptions.runtime;

public class InvalidOffsetException extends NakadiRuntimeBaseException {

    public InvalidOffsetException(final String message) {
        super(message);
    }
}
