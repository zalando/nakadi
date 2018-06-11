package org.zalando.nakadi.exceptions.runtime;

public class NoSuchOwningApplicationException extends NakadiRuntimeBaseException {

    public NoSuchOwningApplicationException(final String message) {
        super(message);
    }
}
