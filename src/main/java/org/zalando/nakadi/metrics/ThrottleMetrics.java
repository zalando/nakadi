package org.zalando.nakadi.metrics;

import com.codahale.metrics.Histogram;

public class ThrottleMetrics {

    private final Histogram bytes;
    private final Histogram messages;
    private final Histogram batches;

    public ThrottleMetrics(Histogram bytes, Histogram messages, Histogram batches) {
        this.bytes = bytes;
        this.messages = messages;
        this.batches = batches;
    }

    public Histogram getBytes() {
        return bytes;
    }

    public Histogram getMessages() {
        return messages;
    }

    public Histogram getBatches() {
        return batches;
    }

    public void mark(final long size, final long messageCount) {
        bytes.update(size);
        messages.update(messageCount);
        batches.update(1);
    }
}
