package org.zalando.nakadi.metrics;

import com.codahale.metrics.MetricRegistry;

import java.util.concurrent.atomic.AtomicInteger;

public class MetricUtils {

    public static final String NAKADI_PREFIX = "nakadi.";
    public static final String EVENTTYPES_PREFIX = NAKADI_PREFIX + "eventtypes";
    public static final String SUBSCRIPTION_PREFIX = NAKADI_PREFIX + "subscriptions";
    private static final AtomicInteger CONSUMER_CLIENT_ID_SEQUENCE = new AtomicInteger(1);

    public static String metricNameFor(final String eventTypeName, final String metricName) {
        return MetricRegistry.name(EVENTTYPES_PREFIX, eventTypeName.replace('.', '#'), metricName);
    }

    public static String metricNameForSubscription(final String subscriptionId, final String metricName) {
        return MetricRegistry.name(SUBSCRIPTION_PREFIX, subscriptionId, metricName);
    }

    public static String getSequenceNumber() {
        return String.valueOf(CONSUMER_CLIENT_ID_SEQUENCE.getAndIncrement());
    }

}
