package de.zalando.aruha.nakadi.metrics;

public class MetricUtils {

    public static final String METRIC_PREFIX = "nakadi.eventtypes.";

    public static String metricNameFor(final String eventTypeName, final String metricName) {
        return METRIC_PREFIX + eventTypeName.replace('.', '#') + "." + metricName;
    }

}
