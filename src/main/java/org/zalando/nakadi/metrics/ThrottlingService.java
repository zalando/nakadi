package org.zalando.nakadi.metrics;

import com.codahale.metrics.Histogram;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.SlidingTimeWindowReservoir;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.zalando.nakadi.util.ZkConfigurationService;

import java.util.Arrays;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;

@Component
public class ThrottlingService {

    private static final Duration THROTTLING_PERIOD = Duration.standardMinutes(15);
    private static final String BYTES_LIMIT = "nakadi.throttling.bytesLimit";
    private static final String MESSAGES_LIMIT = "nakadi.throttling.messagesLimit";
    private static final String BATCHES_LIMIT = "nakadi.throttling.batchesLimit";

    private final ConcurrentMap<ThrottleKey, ThrottleMetrics> metrics = new ConcurrentHashMap<>();
    private final MetricRegistry metricRegistry;
    private final ZkConfigurationService zkConfigurationService;

    @Autowired
    public ThrottlingService(final MetricRegistry metricRegistry, final ZkConfigurationService zkConfigurationService) {
        this.metricRegistry = metricRegistry;
        this.zkConfigurationService = zkConfigurationService;
    }

    public ThrottleResult mark(final String application, final String eventType, final int size,
                               final int messagesCount) {
        ThrottleMetrics throttleMetrics = metricsFor(application, eventType);

        ThrottleResult current = getThrottleResult(throttleMetrics);
        if (current.isThrottled()) {
            return current;
        }
        throttleMetrics.mark(size, messagesCount);
        return getThrottleResult(throttleMetrics);
    }

    private ThrottleResult getThrottleResult(ThrottleMetrics metrics) {
        //return metrics for the 15 last minutes
        final long batches = metrics.getBatches().getCount();
        final long bytes = Arrays.stream(metrics.getBytes().getSnapshot().getValues()).sum();
        final long messages = Arrays.stream(metrics.getMessages().getSnapshot().getValues()).sum();

        final long bytesLimit = zkConfigurationService.getLong(BYTES_LIMIT);
        final long messagesLimit = zkConfigurationService.getLong(MESSAGES_LIMIT);
        final long batchesLimit = zkConfigurationService.getLong(BATCHES_LIMIT);

        final long bytesRemaining = remaining(bytesLimit, bytes);
        final long messagesRemaining = remaining(messagesLimit, messages);
        final long batchesRemaining = remaining(batchesLimit, batches);

        Instant reset = Instant.now().plus(THROTTLING_PERIOD);
        return new ThrottleResult(bytesLimit, bytesRemaining, messagesLimit, messagesRemaining, batchesLimit,
                batchesRemaining, reset);
    }

    private long remaining(final long limit, final long value) {
        long remaining = limit - value;
        return remaining <= 0 ? 0 : remaining;
    }

    private ThrottleMetrics metricsFor(final String application, final String eventType) {
        return metrics.computeIfAbsent(ThrottleKey.key(application, eventType),
                key -> {
                    Histogram bytesHistogram = new Histogram(new SlidingTimeWindowReservoir(15, TimeUnit.MINUTES));
                    Histogram batchesHistogram = new Histogram(new SlidingTimeWindowReservoir(15, TimeUnit.MINUTES));
                    Histogram messagesHistogram = new Histogram(new SlidingTimeWindowReservoir(15, TimeUnit.MINUTES));
                    metricRegistry.register(MetricUtils.throttlingMetricNameFor(application, eventType, "bytes"),
                            bytesHistogram);
                    metricRegistry.register(MetricUtils.throttlingMetricNameFor(application, eventType, "messages"),
                            messagesHistogram);
                    metricRegistry.register(MetricUtils.throttlingMetricNameFor(application, eventType, "batches"),
                            batchesHistogram);
                    return new ThrottleMetrics(bytesHistogram, messagesHistogram, batchesHistogram);
                });
    }

    private static class ThrottleKey {

        private final String eventType;
        private final String application;

        public static ThrottleKey key(String application, String eventType) {
            return new ThrottleKey(eventType, application);
        }

        private ThrottleKey(String eventType, String application) {
            this.eventType = eventType;
            this.application = application;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            ThrottleKey that = (ThrottleKey) o;
            return Objects.equals(eventType, that.eventType) &&
                    Objects.equals(application, that.application);
        }

        @Override
        public int hashCode() {
            return Objects.hash(eventType, application);
        }
    }
}
