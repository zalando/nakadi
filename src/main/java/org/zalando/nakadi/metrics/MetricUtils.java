package org.zalando.nakadi.metrics;

import com.codahale.metrics.MetricRegistry;

public class MetricUtils {

    public static final String NAKADI_PREFIX = "nakadi.";
    public static final String EVENTTYPES_PREFIX = NAKADI_PREFIX + "eventtypes";
    public static final String SUBSCRIPTION_PREFIX = NAKADI_PREFIX + "subscriptions";
    private static final String LOW_LEVEL_STREAM = "lola";
    private static final String HIGH_LEVEL_STREAM = "hila";
    private static final String BYTES_FLUSHED = "bytes-flushed";

    public static String metricNameFor(final String eventTypeName, final String metricName) {
        return MetricRegistry.name(EVENTTYPES_PREFIX, eventTypeName.replace('.', '#'), metricName);
    }

    public static String metricNameForSubscription(final String subscriptionId, final String metricName) {
        return MetricRegistry.name(SUBSCRIPTION_PREFIX, subscriptionId, metricName);
    }

    public static String metricNameForLoLAStream(final String applicationId, final String eventTypeName) {
        return MetricRegistry.name(
                LOW_LEVEL_STREAM,
                applicationId.replace(".", "#"),
                eventTypeName.replace(".", "#"),
                BYTES_FLUSHED);
    }

    public static String metricNameForHiLAStream(final String applicationId, final String subscriptionId) {
        return MetricRegistry.name(
                HIGH_LEVEL_STREAM,
                applicationId.replace(".", "#"),
                subscriptionId,
                BYTES_FLUSHED);
    }
}
