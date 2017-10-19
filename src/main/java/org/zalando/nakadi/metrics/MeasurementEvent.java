package org.zalando.nakadi.metrics;

import java.util.Date;

public class MeasurementEvent {
    public static class MeasurementStep {
        private String name;
        private int count;
        private long timeTotalMs;

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public int getCount() {
            return count;
        }

        public void setCount(int count) {
            this.count = count;
        }

        public long getTimeTotalMs() {
            return timeTotalMs;
        }

        public void setTimeTotalMs(long timeTotalMs) {
            this.timeTotalMs = timeTotalMs;
        }
    }

    private String event;
    private Date startedAt;
    private Date endedAt;
    private long durationMillis;
    private MeasurementStep[] steps;

    public String getEvent() {
        return event;
    }

    public void setEvent(String event) {
        this.event = event;
    }

    public Date getStartedAt() {
        return startedAt;
    }

    public void setStartedAt(Date startedAt) {
        this.startedAt = startedAt;
    }

    public Date getEndedAt() {
        return endedAt;
    }

    public void setEndedAt(Date endedAt) {
        this.endedAt = endedAt;
    }

    public long getDurationMillis() {
        return durationMillis;
    }

    public void setDurationMillis(long durationMillis) {
        this.durationMillis = durationMillis;
    }

    public MeasurementStep[] getSteps() {
        return steps;
    }

    public void setSteps(MeasurementStep[] steps) {
        this.steps = steps;
    }
}
