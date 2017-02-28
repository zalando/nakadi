package org.zalando.nakadi.metrics;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.Metric;
import com.codahale.metrics.MetricSet;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.metrics.KafkaMetric;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class KafkaClientMetrics implements MetricSet {
    private final Consumer<String, String> kafkaConsumer;

    public KafkaClientMetrics(final Consumer<String, String> kafkaConsumer) {
        this.kafkaConsumer = kafkaConsumer;
    }

    @Override
    public Map<String, Metric> getMetrics() {
        final Map<String, Metric> gauges = new HashMap<>();
        for (final Object metricObject : kafkaConsumer.metrics().entrySet()) {
            final Map.Entry<MetricName, KafkaMetric> metricEntry = (Map.Entry<MetricName, KafkaMetric>) metricObject;
            gauges.put(metricEntry.getKey().name(), (Gauge) () -> { return metricEntry.getValue().value(); });
        }
        return Collections.unmodifiableMap(gauges);
    }
}
