package org.zalando.nakadi.exceptions.runtime;

public class ServiceTemporaryUnavailableException extends MyNakadiRuntimeException1 {
    public ServiceTemporaryUnavailableException(final Exception cause) {
        super(cause.getMessage(), cause);
    }
}
