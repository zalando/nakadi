package org.zalando.nakadi.throttling;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class SampledTotal {

    private int current = 0;

    private List<Sample> samples;
    private final long windowMs;
    private final int samplesCount;
    private final long expireAge;

    public SampledTotal(final long windowMs, final int samplesAmount) {
        this.windowMs = windowMs;
        this.samplesCount = samplesAmount;
        this.samples = new ArrayList<>(samplesAmount);
        this.expireAge = samplesAmount * windowMs;
    }

    public void record(double value, long timeMs) {
        Sample sample = current(timeMs);
        if (sample.isComplete(timeMs, windowMs)) {
            sample = advance(timeMs);
        }
        samples.set(current, update(sample, value, timeMs));
    }

    private Sample advance(long timeMs) {
        current = (current + 1) % samplesCount;
        if (this.current >= samples.size()) {
            Sample sample = newSample(timeMs);
            samples.add(sample);
            return sample;
        } else {
            Sample sample = current(timeMs);
            samples.set(current, newSample(timeMs));
            return sample;
        }
    }

    public double measure(long now) {
        purgeObsoleteSamples(now);
        return combine(samples);
    }

    private Sample current(long timeMs) {
        if (samples.size() == 0) {
            samples.add(newSample(timeMs));
        }
        return this.samples.get(current);
    }


    private Sample update(Sample sample, double value, long now) {
        return new Sample(sample.getValue() + value, now);
    }

    private double combine(List<Sample> samples) {
        return samples.stream().mapToDouble(Sample::getValue).sum();
    }

    private Sample newSample(long timeMs) {
        return new Sample(0, timeMs);
    }

    private void purgeObsoleteSamples(long now) {
        samples = samples.stream().map(sample -> sample.isExpire(now, expireAge) ? newSample(now) : sample)
                .collect(Collectors.toList());
    }

    private static class Sample {

        private final long lastWindowMs;
        private final double value;

        public Sample(double value, long now) {
            this.lastWindowMs = now;
            this.value = value;
        }

        public boolean isComplete(long timeMs, long windowMs) {
            return timeMs - lastWindowMs >= windowMs;
        }

        public boolean isExpire(long now, long expireAge) {
            return now - lastWindowMs >= expireAge;
        }

        public double getValue() {
            return value;
        }
    }

}
