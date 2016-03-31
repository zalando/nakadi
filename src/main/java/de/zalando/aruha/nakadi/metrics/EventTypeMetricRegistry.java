package de.zalando.aruha.nakadi.metrics;

import com.codahale.metrics.MetricRegistry;

import java.util.HashMap;
import java.util.Map;

public class EventTypeMetricRegistry {

    private final Map<String, EventTypeMetrics> metricsPerEventType = new HashMap<>();
    private final MetricRegistry metricRegistry;


    public EventTypeMetricRegistry(final MetricRegistry metricRegistry) {
        this.metricRegistry = metricRegistry;
    }

    public EventTypeMetrics metricsFor(final String eventTypeName) {
        EventTypeMetrics eventTypeMetrics = metricsPerEventType.get(eventTypeName);
        if (eventTypeMetrics == null) {
            synchronized (metricsPerEventType) {
                eventTypeMetrics = getOrAddMetricsFor(eventTypeName);
            }
        }
        return eventTypeMetrics;
    }

    private EventTypeMetrics getOrAddMetricsFor(final String eventTypeName) {
        EventTypeMetrics eventTypeMetrics = metricsPerEventType.get(eventTypeName);
        if (eventTypeMetrics == null) {
            eventTypeMetrics = new EventTypeMetrics(eventTypeName, metricRegistry);
            metricsPerEventType.put(eventTypeName, eventTypeMetrics);
        }
        return eventTypeMetrics;
    }
}
