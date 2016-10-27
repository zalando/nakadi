package org.zalando.nakadi.throttling;

import java.util.concurrent.TimeUnit;

public class ThrottleMetrics {

    private final SampledTotal byteCount;
    private final SampledTotal messageCount;
    private final SampledTotal batchCount;

    public ThrottleMetrics(final long windowLengthMs, final int sampleCount) {
        this.byteCount = new SampledTotal(windowLengthMs, TimeUnit.MILLISECONDS, sampleCount);
        this.messageCount = new SampledTotal(windowLengthMs, TimeUnit.MILLISECONDS, sampleCount);
        this.batchCount = new SampledTotal(windowLengthMs, TimeUnit.MILLISECONDS, sampleCount);
    }

    public SampledTotal getByteCount() {
        return byteCount;
    }

    public SampledTotal getMessageCount() {
        return messageCount;
    }

    public SampledTotal getBatchCount() {
        return batchCount;
    }

    public void mark(final long byteCount, final long messageCount, final long now) {
        this.byteCount.record(byteCount, now);
        this.messageCount.record(messageCount, now);
        this.batchCount.record(1L, now);
    }
}
