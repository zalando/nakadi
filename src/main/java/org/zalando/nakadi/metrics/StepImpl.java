package org.zalando.nakadi.metrics;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class StepImpl implements MetricsCollector.Step {
    private final String name;
    private int count;
    private long overallDurationNs;
    private long lastStartNs;
    private static final Logger LOG = LoggerFactory.getLogger(StepImpl.class);

    StepImpl(final String name) {
        this.name = name;
        this.count = 0;
        this.overallDurationNs = 0L;
        this.lastStartNs = 0L;
    }

    void start() {
        this.lastStartNs = System.nanoTime();
    }

    boolean isStarted() {
        return 0L != this.lastStartNs;
    }

    @Override
    public void close() {
        LOG.info("Closing {}", name);
        final long currentTimeNs = System.nanoTime();
        this.overallDurationNs += currentTimeNs - this.lastStartNs;
        ++this.count;
        this.lastStartNs = 0L;
    }

    public String getName() {
        return name;
    }

    public int getCount() {
        return count;
    }

    public long getOverallDurationNs() {
        return overallDurationNs;
    }
}
