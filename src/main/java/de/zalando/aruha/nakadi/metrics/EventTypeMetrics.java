package de.zalando.aruha.nakadi.metrics;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.google.common.annotations.VisibleForTesting;
import static de.zalando.aruha.nakadi.metrics.MetricUtils.metricNameFor;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;

public class EventTypeMetrics {

    private final String eventTypeName;
    private final MetricRegistry metricRegistry;

    private final Histogram eventsPerBatchHistogram;
    private final Timer publishingTimer;
    private final Meter eventCountMeter;
    private final Histogram averageEventSizeInBytesHistogram;
    private final ConcurrentMap<Integer, Counter> statusCodeCounter = new ConcurrentHashMap<>();

    public EventTypeMetrics(final String eventTypeName, final MetricRegistry metricRegistry) {
        this.eventTypeName = eventTypeName;
        this.metricRegistry = metricRegistry;
        eventCountMeter = metricRegistry.meter(metricNameFor(eventTypeName, "publishing.events"));
        eventsPerBatchHistogram = metricRegistry.histogram(metricNameFor(eventTypeName, "publishing.eventsPerBatch"));
        averageEventSizeInBytesHistogram = metricRegistry.histogram(metricNameFor(eventTypeName, "publishing.averageEventSizeInBytes"));
        publishingTimer = metricRegistry.timer(metricNameFor(eventTypeName, "publishing"));
    }

    public void reportSizing(int eventsPerBatch, int totalEventSize) {
        eventsPerBatchHistogram.update(eventsPerBatch);
        eventCountMeter.mark(eventsPerBatch);
        averageEventSizeInBytesHistogram.update(eventsPerBatch == 0 ? 0 : totalEventSize / eventsPerBatch);
    }

    public void incrementResponseCount(int code) {
        statusCodeCounter.computeIfAbsent(code,
                key -> metricRegistry.counter(metricNameFor(eventTypeName, "publishing." + code)))
                .inc();
    }

    public void updateTiming(long startingNanos, long currentNanos) {
        publishingTimer.update(currentNanos - startingNanos, TimeUnit.NANOSECONDS);
    }

    @VisibleForTesting
    public long getResponseCount(int code) {
        return Optional.ofNullable(statusCodeCounter.get(code)).map(Counter::getCount).orElse(-1L);
    }
}
