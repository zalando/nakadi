package org.zalando.nakadi.exceptions.runtime;

public class ServiceTemporarilyUnavailableException extends NakadiBaseException {
    public ServiceTemporarilyUnavailableException(final Exception cause) {
        super(cause.getMessage(), cause);
    }

    public ServiceTemporarilyUnavailableException(final String message) {
        super(message);
    }

    public ServiceTemporarilyUnavailableException(final String message, final Throwable cause) {
        super(message, cause);
    }
}
