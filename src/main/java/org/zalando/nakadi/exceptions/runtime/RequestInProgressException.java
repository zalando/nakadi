package org.zalando.nakadi.exceptions.runtime;

public class RequestInProgressException extends NakadiRuntimeBaseException {

    public RequestInProgressException(final String message, final Throwable cause) {
        super(message, cause);
    }

}
