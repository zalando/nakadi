package org.zalando.nakadi.exceptions.runtime;

public class IllegalClientIdException extends NakadiRuntimeBaseException {

    public IllegalClientIdException(final String message) {
        super(message);
    }

}
