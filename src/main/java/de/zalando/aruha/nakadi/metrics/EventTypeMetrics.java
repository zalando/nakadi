package de.zalando.aruha.nakadi.metrics;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;

import static de.zalando.aruha.nakadi.metrics.MetricUtils.metricNameFor;

public class EventTypeMetrics {

    private final Histogram eventsPerBatchHistogram;
    private final Timer successfullyPublishedTimer;
    private final Counter failedPublishedCounter;
    private final Histogram averageEventSizeInBytesHistogram;

    public EventTypeMetrics(final String eventTypeName, final MetricRegistry metricRegistry) {
        eventsPerBatchHistogram = metricRegistry.histogram(metricNameFor(eventTypeName, "publishing.eventsPerBatch"));
        averageEventSizeInBytesHistogram = metricRegistry.histogram(metricNameFor(eventTypeName, "publishing.averageEventSizeInBytes"));
        successfullyPublishedTimer = metricRegistry.timer(metricNameFor(eventTypeName, "publishing.successfully"));
        failedPublishedCounter = metricRegistry.counter(metricNameFor(eventTypeName, "publishing.failed"));
    }

    public Histogram getEventsPerBatchHistogram() {
        return eventsPerBatchHistogram;
    }

    public Timer getSuccessfullyPublishedTimer() {
        return successfullyPublishedTimer;
    }

    public Counter getFailedPublishedCounter() {
        return failedPublishedCounter;
    }

    public Histogram getAverageEventSizeInBytesHistogram() {
        return averageEventSizeInBytesHistogram;
    }
}
