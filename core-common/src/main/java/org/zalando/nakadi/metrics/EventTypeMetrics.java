package org.zalando.nakadi.metrics;

import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.google.common.annotations.VisibleForTesting;

import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;

import static org.zalando.nakadi.metrics.MetricUtils.metricNameFor;

public class EventTypeMetrics {

    private final String eventTypeName;
    private final MetricRegistry metricRegistry;

    private final Histogram eventsPerBatchHistogram;
    private final Timer publishingTimer;
    private final Meter eventCountMeter;
    private final Meter trafficInBytesMeter;
    private final Histogram batchSizeInBytesHistogram;
    private final Histogram averageEventSizeInBytesHistogram;
    private final ConcurrentMap<Integer, Meter> statusCodeMeter = new ConcurrentHashMap<>();

    public EventTypeMetrics(final String eventTypeName, final MetricRegistry metricRegistry) {
        this.eventTypeName = eventTypeName;
        this.metricRegistry = metricRegistry;
        eventCountMeter = metricRegistry.meter(metricNameFor(eventTypeName, "publishing.events"));
        trafficInBytesMeter = metricRegistry.meter(metricNameFor(eventTypeName, "publishing.trafficInBytes"));
        batchSizeInBytesHistogram = metricRegistry.histogram(
                metricNameFor(eventTypeName, "publishing.batchSizeInBytes"));
        eventsPerBatchHistogram = metricRegistry.histogram(metricNameFor(eventTypeName, "publishing.eventsPerBatch"));
        averageEventSizeInBytesHistogram = metricRegistry.histogram(
                metricNameFor(eventTypeName, "publishing.averageEventSizeInBytes"));
        publishingTimer = metricRegistry.timer(metricNameFor(eventTypeName, "publishing"));
    }

    public void reportSizing(final int eventsPerBatch, final long totalEventSize) {
        eventsPerBatchHistogram.update(eventsPerBatch);
        eventCountMeter.mark(eventsPerBatch);
        trafficInBytesMeter.mark(totalEventSize);
        batchSizeInBytesHistogram.update(totalEventSize);
        averageEventSizeInBytesHistogram.update(eventsPerBatch == 0 ? 0 : totalEventSize / eventsPerBatch);
    }

    public void incrementResponseCount(final int code) {
        statusCodeMeter.computeIfAbsent(code,
                key -> metricRegistry.meter(metricNameFor(eventTypeName, "publishing." + code)))
                .mark();
    }

    public void updateTiming(final long startingNanos, final long currentNanos) {
        publishingTimer.update(currentNanos - startingNanos, TimeUnit.NANOSECONDS);
    }

    @VisibleForTesting
    public long getResponseCount(final int code) {
        return Optional.ofNullable(statusCodeMeter.get(code)).map(Meter::getCount).orElse(-1L);
    }
}
