package de.zalando.aruha.nakadi.metrics;

import com.codahale.metrics.MetricRegistry;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class EventTypeMetricRegistry {

    private final ConcurrentMap<String, EventTypeMetrics> metricsPerEventType = new ConcurrentHashMap<>();
    private final MetricRegistry metricRegistry;


    public EventTypeMetricRegistry(final MetricRegistry metricRegistry) {
        this.metricRegistry = metricRegistry;
    }

    public EventTypeMetrics metricsFor(final String eventTypeName) {
        return metricsPerEventType.computeIfAbsent(eventTypeName,
                key -> new EventTypeMetrics(eventTypeName, metricRegistry));
    }

}
