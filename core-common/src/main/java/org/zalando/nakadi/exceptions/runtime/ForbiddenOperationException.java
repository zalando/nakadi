package org.zalando.nakadi.exceptions.runtime;

public class ForbiddenOperationException extends NakadiBaseException {

    public ForbiddenOperationException(final String message) {
        super(message);
    }
}
