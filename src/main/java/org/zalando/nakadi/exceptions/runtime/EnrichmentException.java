package org.zalando.nakadi.exceptions.runtime;

public class EnrichmentException extends NakadiBaseException {
    public EnrichmentException(final String msg, final Exception cause) {
        super(msg, cause);
    }

    public EnrichmentException(final String msg) {
        super(msg);
    }
}
