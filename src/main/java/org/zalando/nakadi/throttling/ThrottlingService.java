package org.zalando.nakadi.throttling;

import org.joda.time.Instant;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.zalando.nakadi.util.ZkConfigurationService;

import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

@Component
public class ThrottlingService {

    private static final String BYTES_LIMIT = "nakadi.throttling.bytesLimit";
    private static final String MESSAGES_LIMIT = "nakadi.throttling.messagesLimit";
    private static final String BATCHES_LIMIT = "nakadi.throttling.batchesLimit";
    private final long windowSize;
    private final int samples;

    private final ConcurrentMap<ThrottleKey, ThrottleMetrics> metrics = new ConcurrentHashMap<>();
    private final ZkConfigurationService zkConfigurationService;

    @Autowired
    public ThrottlingService(@Value("${nakadi.throttling.windowSize}") final long windowSize,
                             @Value("${nakadi.throttling.samples}") final int samples,
                             final ZkConfigurationService zkConfigurationService) {
        this.windowSize = windowSize;
        this.samples = samples;
        this.zkConfigurationService = zkConfigurationService;
    }

    public ThrottleResult mark(final String application, final String eventType, final int size,
                               final int messagesCount) {
        final long now = Instant.now().getMillis();
        final ThrottleMetrics throttleMetrics = metricsFor(application, eventType);

        final ThrottleResult current = getThrottleResult(throttleMetrics, now);
        if (current.isThrottled()) {
            return current;
        }
        throttleMetrics.mark(size, messagesCount, now);
        return getThrottleResult(throttleMetrics, now);
    }

    public ThrottleResult current(final String application, final String eventType) {
        final long now = Instant.now().getMillis();
        final ThrottleMetrics throttleMetrics = metricsFor(application, eventType);
        return getThrottleResult(throttleMetrics, now);
    }

    private ThrottleResult getThrottleResult(final ThrottleMetrics metrics, final long now) {
        //return metrics for the 15 last minutes
        final long batches = metrics.getBatches().measure(now);
        final long bytes = metrics.getBytes().measure(now);
        final long messages = metrics.getMessages().measure(now);

        final long bytesLimit = zkConfigurationService.getLong(BYTES_LIMIT);
        final long messagesLimit = zkConfigurationService.getLong(MESSAGES_LIMIT);
        final long batchesLimit = zkConfigurationService.getLong(BATCHES_LIMIT);

        final long bytesRemaining = remaining(bytesLimit, bytes);
        final long messagesRemaining = remaining(messagesLimit, messages);
        final long batchesRemaining = remaining(batchesLimit, batches);

        //TODO calculate correct reset time
        final Instant reset = Instant.now();
        return new ThrottleResult(bytesLimit, bytesRemaining, messagesLimit, messagesRemaining, batchesLimit,
                batchesRemaining, reset);
    }

    private long remaining(final long limit, final long value) {
        final long remaining = limit - value;
        return remaining <= 0 ? 0 : remaining;
    }

    private ThrottleMetrics metricsFor(final String application, final String eventType) {
        return metrics.computeIfAbsent(ThrottleKey.key(application, eventType),
                key -> new ThrottleMetrics(windowSize, samples));
    }

    private static class ThrottleKey {

        private final String eventType;
        private final String application;

        public static ThrottleKey key(final String application, final String eventType) {
            return new ThrottleKey(eventType, application);
        }

        private ThrottleKey(final String eventType, final String application) {
            this.eventType = eventType;
            this.application = application;
        }

        @Override
        public boolean equals(final Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            final ThrottleKey that = (ThrottleKey) o;
            return Objects.equals(eventType, that.eventType) &&
                    Objects.equals(application, that.application);
        }

        @Override
        public int hashCode() {
            return Objects.hash(eventType, application);
        }
    }
}
