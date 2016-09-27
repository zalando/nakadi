package org.zalando.nakadi.metrics;

import org.joda.time.Instant;

public class ThrottleResult {

    private final long bytesLimit;
    private final long bytesRemaining;
    private final long messagesLimit;
    private final long messagesRemaining;
    private final long batchesLimit;
    private final long batchesRemaining;
    private final Instant reset;

    public ThrottleResult(final long bytesLimit, final long bytesRemaining, final long messagesLimit,
                          final long messagesRemaining, final long batchesLimit, final long batchesRemaining,
                          final Instant reset) {
        this.bytesLimit = bytesLimit;
        this.bytesRemaining = bytesRemaining;
        this.messagesLimit = messagesLimit;
        this.messagesRemaining = messagesRemaining;
        this.batchesLimit = batchesLimit;
        this.batchesRemaining = batchesRemaining;
        this.reset = reset;
    }

    public long getBytesLimit() {
        return bytesLimit;
    }

    public long getBytesRemaining() {
        return bytesRemaining;
    }

    public long getMessagesLimit() {
        return messagesLimit;
    }

    public long getMessagesRemaining() {
        return messagesRemaining;
    }

    public long getBatchesLimit() {
        return batchesLimit;
    }

    public long getBatchesRemaining() {
        return batchesRemaining;
    }

    public Instant getReset() {
        return reset;
    }

    public boolean isThrottled() {
        return batchesRemaining == 0 || bytesRemaining == 0 || messagesRemaining == 0;
    }
}
