package org.zalando.nakadi.throttling;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

public class SampledTotal {

    private final ConcurrentHashMap<Long, Long> samples;
    private final long windowLengthMs;
    private final long expireAge;

    public SampledTotal(final long windowLength, final TimeUnit unit, final int samplesAmount) {
        this.windowLengthMs = unit.toMillis(windowLength);
        this.samples = new ConcurrentHashMap<>();
        this.expireAge = samplesAmount * windowLength;
    }

    public void record(final Long value, final long timeMs) {
        final long timeBucket = (timeMs / windowLengthMs) * windowLengthMs;
        samples.compute(timeBucket, (k, v) -> {
            if (v == null) {
                return 1L;
            } else {
                return v + value;
            }});
    }

    public Long measure(final long now) {
        cleanup(now);
        return combine();
    }

    private long combine() {
        return samples.values().stream().mapToLong(v -> v).sum();
    }

    private void cleanup(final long now) {
        samples.keySet().stream().filter(key -> key <= now - expireAge).forEach(samples::remove);
    }

}
