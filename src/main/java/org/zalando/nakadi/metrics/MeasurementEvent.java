package org.zalando.nakadi.metrics;

import java.util.Map;

public class MeasurementEvent {
    public static class MeasurementStep {
        private String name;
        private int count;
        private long timeTotalMs;

        public String getName() {
            return name;
        }

        public void setName(final String name) {
            this.name = name;
        }

        public int getCount() {
            return count;
        }

        public void setCount(final int count) {
            this.count = count;
        }

        public long getTimeTotalMs() {
            return timeTotalMs;
        }

        public void setTimeTotalMs(final long timeTotalMs) {
            this.timeTotalMs = timeTotalMs;
        }
    }

    private Map<String, String> additional;
    private long startedAt;
    private long endedAt;
    private long durationMillis;
    private MeasurementStep[] steps;

    public Map<String, String> getAdditional() {
        return additional;
    }

    public void setAdditional(final Map<String, String> additional) {
        this.additional = additional;
    }

    public long getStartedAt() {
        return startedAt;
    }

    public void setStartedAt(final long startedAt) {
        this.startedAt = startedAt;
    }

    public long getEndedAt() {
        return endedAt;
    }

    public void setEndedAt(final long endedAt) {
        this.endedAt = endedAt;
    }

    public long getDurationMillis() {
        return durationMillis;
    }

    public void setDurationMillis(final long durationMillis) {
        this.durationMillis = durationMillis;
    }

    public MeasurementStep[] getSteps() {
        return steps;
    }

    public void setSteps(final MeasurementStep[] steps) {
        this.steps = steps;
    }
}
