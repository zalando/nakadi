package org.zalando.nakadi.metrics;

import com.codahale.metrics.MetricRegistry;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

@Component
public class EventTypeMetricRegistry {

    private final ConcurrentMap<String, EventTypeMetrics> metricsPerEventType = new ConcurrentHashMap<>();
    private final MetricRegistry metricRegistry;

    @Autowired
    public EventTypeMetricRegistry(final MetricRegistry metricRegistry) {
        this.metricRegistry = metricRegistry;
    }

    public EventTypeMetrics metricsFor(final String eventTypeName) {
        return metricsPerEventType.computeIfAbsent(eventTypeName,
                key -> new EventTypeMetrics(eventTypeName, metricRegistry));
    }

}
