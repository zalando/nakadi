package org.zalando.nakadi.throttling;

import org.joda.time.Instant;

public class ThrottleResult {

    private final long bytesLimit;
    private final long bytesRemaining;
    private final Instant resetAt;
    private final boolean throttled;

    public ThrottleResult(final long bytesLimit, final long bytesRemaining, final Instant resetAt,
                          final boolean throttled) {
        this.bytesLimit = bytesLimit;
        this.bytesRemaining = bytesRemaining;
        this.resetAt = resetAt;
        this.throttled = throttled;
    }

    public long getBytesLimit() {
        return bytesLimit;
    }

    public long getBytesRemaining() {
        return bytesRemaining;
    }

    public Instant getResetAt() {
        return resetAt;
    }

    public boolean isThrottled() {
        return throttled;
    }
}
