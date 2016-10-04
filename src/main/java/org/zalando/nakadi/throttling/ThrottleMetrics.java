package org.zalando.nakadi.throttling;

public class ThrottleMetrics {

    private final SampledTotal bytes;
    private final SampledTotal messages;
    private final SampledTotal batches;

    public ThrottleMetrics(final long windowSize, final int samples) {
        this.bytes = new SampledTotal(windowSize, samples);
        this.messages = new SampledTotal(windowSize, samples);
        this.batches = new SampledTotal(windowSize, samples);
    }

    public SampledTotal getBytes() {
        return bytes;
    }

    public SampledTotal getMessages() {
        return messages;
    }

    public SampledTotal getBatches() {
        return batches;
    }

    public void mark(final long size, final long messageCount, final long now) {
        bytes.record(size, now);
        messages.record(messageCount, now);
        batches.record(1, now);
    }
}
