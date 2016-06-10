package de.zalando.aruha.nakadi.service.subscription;

public class SubscriptionWrappedException extends RuntimeException {
    private final Exception sourceException;

    public SubscriptionWrappedException(final Exception sourceException) {
        this.sourceException = sourceException;
    }

    public Exception getSourceException() {
        return sourceException;
    }
}
