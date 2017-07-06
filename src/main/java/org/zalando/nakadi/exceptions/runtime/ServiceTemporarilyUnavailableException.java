package org.zalando.nakadi.exceptions.runtime;

public class ServiceTemporarilyUnavailableException extends MyNakadiRuntimeException1 {
    public ServiceTemporarilyUnavailableException(final Exception cause) {
        super(cause.getMessage(), cause);
    }

    public ServiceTemporarilyUnavailableException(final String message, final Throwable cause) {
        super(message, cause);
    }
}
