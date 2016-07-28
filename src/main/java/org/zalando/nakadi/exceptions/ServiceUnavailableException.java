package org.zalando.nakadi.exceptions;

import javax.ws.rs.core.Response;

public class ServiceUnavailableException extends NakadiException {
    public ServiceUnavailableException(final String message) {
        super(message);
    }

    public ServiceUnavailableException(final String msg, final Exception cause) {
        super(msg, cause);
    }

    public ServiceUnavailableException(final String msg, final String problemMessage, final Exception cause) {
        super(msg, problemMessage, cause);
    }

    @Override
    protected Response.StatusType getStatus() {
        return Response.Status.SERVICE_UNAVAILABLE;
    }
}
