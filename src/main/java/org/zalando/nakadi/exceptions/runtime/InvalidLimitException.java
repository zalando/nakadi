package org.zalando.nakadi.exceptions.runtime;

public class InvalidLimitException extends NakadiRuntimeBaseException {

    public InvalidLimitException(final String message) {
        super(message);
    }
}
