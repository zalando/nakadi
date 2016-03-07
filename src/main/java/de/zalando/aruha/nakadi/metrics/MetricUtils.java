package de.zalando.aruha.nakadi.metrics;

public class MetricUtils {

    public static final String NAKADI_PREFIX = "nakadi.";
    public static final String EVENTTYPES_PREFIX = NAKADI_PREFIX + "eventtypes.";

    public static String metricNameFor(final String eventTypeName, final String metricName) {
        return EVENTTYPES_PREFIX + eventTypeName.replace('.', '#') + "." + metricName;
    }

}
