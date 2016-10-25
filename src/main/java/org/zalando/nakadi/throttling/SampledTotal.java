package org.zalando.nakadi.throttling;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

public class SampledTotal {

    private final ConcurrentHashMap<Long, AtomicLong> samples;
    private final long windowMs;
    private final long expireAge;

    public SampledTotal(final long windowMs, final int samplesAmount) {
        this.windowMs = windowMs;
        this.samples = new ConcurrentHashMap<>();
        this.expireAge = samplesAmount * windowMs;
    }

    public void record(final Long value, final long timeMs) {
        final AtomicLong sample = current(timeMs);
        sample.addAndGet(value);
    }

    public Long measure(final long now) {
        cleanup(now);
        return combine();
    }

    private AtomicLong current(final long timeMs) {
        final long timeBucket = (timeMs / windowMs) * windowMs;
        return samples.computeIfAbsent(timeBucket, key -> new AtomicLong());
    }

    private long combine() {
        return samples.values().stream().mapToLong(AtomicLong::get).sum();
    }

    private void cleanup(final long now) {
        samples.keySet().stream().filter(key -> key <= now - expireAge).forEach(samples::remove);
    }

}
