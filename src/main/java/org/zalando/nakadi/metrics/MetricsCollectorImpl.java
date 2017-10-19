package org.zalando.nakadi.metrics;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

class MetricsCollectorImpl implements MetricsCollector {
    private final String et;
    private final Map<String, String> additional;
    private final Consumer<MetricsCollectorImpl> finishCallback;
    private final List<StepImpl> steps;
    private final long start;
    private long finish;
    private static final Logger LOG = LoggerFactory.getLogger(MetricsCollectorImpl.class);

    MetricsCollectorImpl(
            final String et,
            final Map<String, String> additional,
            final Consumer<MetricsCollectorImpl> finishCallback) {
        this.et = et;
        this.additional = additional;
        this.start = System.currentTimeMillis();
        this.finishCallback = finishCallback;
        this.steps = new ArrayList<>();
    }

    public String getEt() {
        return et;
    }

    public Map<String, String> getAdditional() {
        return additional;
    }

    public List<StepImpl> getSteps() {
        return steps;
    }

    public long getStart() {
        return start;
    }

    public long getFinish() {
        return finish;
    }

    @Override
    public void closeLast() {
        for (int i = steps.size(); i > 0; --i) {
            final StepImpl step = steps.get(i - 1);
            if (step.isStarted()) {
                step.close();
                return;
            }
        }
    }

    @Override
    public Step start(final String name, final boolean autoClose) {
        LOG.info("Starting {}, autoclose: {}", name, autoClose);
        boolean needClose = autoClose;
        StepImpl stepToUse = null;
        for (int i = steps.size(); i > 0; --i) {
            final StepImpl step = steps.get(i - 1);
            if (needClose && step.isStarted()) {
                step.close();
                needClose = false;
            }
            if (null == stepToUse && step.getName().equals(name)) {
                stepToUse = step;
            }
            if (null != stepToUse && !needClose) {
                break;
            }
        }
        if (null == stepToUse) {
            stepToUse = new StepImpl(name);
            steps.add(stepToUse);
        }
        stepToUse.start();
        return stepToUse;
    }

    @Override
    public void close() {
        this.finish = System.currentTimeMillis();
        this.finishCallback.accept(this);
    }

}
