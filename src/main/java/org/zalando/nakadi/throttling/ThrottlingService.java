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

        ThrottleResult result = getThrottleResult(throttleMetrics, size, messagesCount, now);
        if (!result.isThrottled()) {
            throttleMetrics.mark(size, messagesCount, now);
            return result;
        }
        return result;
    }

    private ThrottleResult getThrottleResult(final ThrottleMetrics metrics, final long size, final long messagesCount,
                                             final long now) {
        //return metrics for the 15 last minutes
        final long batches = metrics.getBatches().measure(now);
        final long bytes = metrics.getBytes().measure(now);
        final long messages = metrics.getMessages().measure(now);

        final long bytesLimit = zkConfigurationService.getLong(BYTES_LIMIT);
        final long messagesLimit = zkConfigurationService.getLong(MESSAGES_LIMIT);
        final long batchesLimit = zkConfigurationService.getLong(BATCHES_LIMIT);

        final long bytesRemaining = bytesLimit - bytes;
        final long messagesRemaining = messagesLimit - messages;
        final long batchesRemaining = batchesLimit - batches;

        final long bytesAfter = bytesRemaining - size;
        final long messagesAfter = messagesRemaining - messagesCount;
        final long batchesAfter = batchesRemaining - 1;

        boolean throttled = batchesAfter < 0 || bytesAfter < 0 || messagesAfter < 0;

        final Instant reset = Instant.now().plus(windowSize);
        if (throttled) {
            return new ThrottleResult(bytesLimit, remaining(bytesRemaining),
                    messagesLimit, remaining(messagesRemaining),
                    batchesLimit, remaining(batchesRemaining),
                    reset, true);
        }
        return new ThrottleResult(bytesLimit, batchesAfter,
                messagesLimit, messagesAfter,
                batchesLimit, batchesAfter,
                reset, false);
    }

    private long remaining(final long value) {
        return value <= 0 ? 0 : value;
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
