package org.zalando.nakadi.metrics;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.zalando.nakadi.exceptions.InternalNakadiException;
import org.zalando.nakadi.repository.EventTypeRepository;

import javax.ws.rs.NotSupportedException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

@Service
public class NakadiKPIMetrics {
    private static final long DUMP_INTERVAL_MS = 1000;
    private static final int EVENT_PACK_SIZE = 1000;
    private static final int QUEUE_SIZE = 100 * EVENT_PACK_SIZE;
    private static final Logger LOG = LoggerFactory.getLogger(NakadiKPIMetrics.class);

    private final ThreadLocal<List<MetricsCollectorImpl>> threadValuesHolder = new ThreadLocal<>();
    private final BlockingQueue<MetricsCollectorImpl> metrics = new LinkedBlockingQueue<>(QUEUE_SIZE);
    private final EventTypeRepository etRepo;

    @Autowired
    public NakadiKPIMetrics(final EventTypeRepository etRepo) {
        this.etRepo = etRepo;
    }

    public MetricsCollector startCollection(final String et, final String name) {
        List<MetricsCollectorImpl> collectors = threadValuesHolder.get();
        if (null == collectors) {
            collectors = new ArrayList<>(2);
            threadValuesHolder.set(collectors);
        }
        final MetricsCollectorImpl result = new MetricsCollectorImpl(et, name, this::collectionFinished);
        collectors.add(result);
        return result;
    }

    public MetricsCollector getCurrent() {
        final List<MetricsCollectorImpl> collectors = threadValuesHolder.get();
        if (null == collectors || collectors.isEmpty()) {
            return FAKE_COLLECTOR;
        }
        return collectors.get(collectors.size() - 1);
    }

    @Scheduled(fixedDelay = DUMP_INTERVAL_MS)
    public void cleanupTimelines() {
        MetricsCollectorImpl item = null;
        final Map<String, List<MeasurementEvent>> groupedByEts = new HashMap<>();
        while (null != (item = metrics.poll())) {
            final List<MeasurementEvent> group = groupedByEts.computeIfAbsent(item.et, k -> new ArrayList<>());
            group.add(toEvent(item));
            if (group.size() > EVENT_PACK_SIZE) {
                dumpToStorage(item.et, group);
                group.clear();
            }
        }
        for (final Map.Entry<String, List<MeasurementEvent>> entry : groupedByEts.entrySet()) {
            if (!entry.getValue().isEmpty()) {
                dumpToStorage(entry.getKey(), entry.getValue());
            }
        }
    }

    private void dumpToStorage(final String et, final List<MeasurementEvent> group) {
        try {
            if (!etRepo.findByNameO(et).isPresent()) {
                registerEventType(et);
            }
        } catch (InternalNakadiException ex) {
            LOG.error("Failed to dump {} nakadi measurement events to {}", group.size(), et);
        }
    }

    private void registerEventType(String et) {
        throw new Not
    }

    private void collectionFinished(final MetricsCollectorImpl item) {
        final List<MetricsCollectorImpl> items = threadValuesHolder.get();
        if (items.remove(item)) {
            final boolean registeredForTermination = metrics.offer(item);
            if (!registeredForTermination) {
                LOG.warn("Metrics collector is skipping record {}", item);
            }
        } else {
            LOG.error("Bug in code, metric {} is removed several times", item);
        }
    }

    private MeasurementEvent toEvent(MetricsCollectorImpl collector) {
        final MeasurementEvent event = new MeasurementEvent();
        event.setStartedAt(new Date(collector.start));
        event.setEndedAt(new Date(collector.finish));
        event.setDurationMillis(collector.finish - collector.start);
        event.setEvent(collector.name);
        final MeasurementEvent.MeasurementStep[] steps = new MeasurementEvent.MeasurementStep[collector.steps.size()];
        for (int i = 0; i < steps.length; ++i) {
            final MeasurementEvent.MeasurementStep mStep = new MeasurementEvent.MeasurementStep();
            final StepImpl cStep = collector.steps.get(i);
            mStep.setCount(cStep.count);
            mStep.setName(cStep.name);
            mStep.setTimeTotalMs(TimeUnit.NANOSECONDS.toMillis(cStep.overallDurationNs));
            steps[i] = mStep;
        }
        event.setSteps(steps);
        return event;
    }

    private static final MetricsCollector.Step FAKE_STEP = () -> {
    };

    private static final MetricsCollector FAKE_COLLECTOR = new MetricsCollector() {
        @Override
        public Step start(final String name, final boolean closePrevious) {
            return FAKE_STEP;
        }

        @Override
        public void close() {
        }
    };

    private static class StepImpl implements MetricsCollector.Step {
        private final String name;
        private int count;
        private long overallDurationNs;
        private long lastStartNs;

        private StepImpl(String name) {
            this.name = name;
            this.count = 0;
            this.overallDurationNs = 0L;
            this.lastStartNs = 0L;
        }

        void start() {
            this.lastStartNs = System.nanoTime();
        }

        boolean isStarted() {
            return 0L == this.lastStartNs;
        }

        @Override
        public void close() {
            final long currentTimeNs = System.nanoTime();
            this.overallDurationNs += currentTimeNs - this.lastStartNs;
            ++this.count;
            this.lastStartNs = 0L;
        }
    }

    private static class MetricsCollectorImpl implements MetricsCollector {
        private final String et;
        private final String name;
        private final Consumer<MetricsCollectorImpl> finishCallback;
        private final List<StepImpl> steps;
        private final long start;
        private long finish;


        private MetricsCollectorImpl(String et, String name, Consumer<MetricsCollectorImpl> finishCallback) {
            this.et = et;
            this.name = name;
            this.start = System.currentTimeMillis();
            this.finishCallback = finishCallback;
            this.steps = new ArrayList<>();
        }

        @Override
        public Step start(final String name, final boolean autoClose) {
            boolean needClose = autoClose;
            StepImpl stepToUse = null;
            for (int i = steps.size(); i > 0; --i) {
                final StepImpl step = steps.get(i - 1);
                if (needClose && step.isStarted()) {
                    step.close();
                    needClose = false;
                }
                if (null == stepToUse && step.name.equals(name)) {
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
}
