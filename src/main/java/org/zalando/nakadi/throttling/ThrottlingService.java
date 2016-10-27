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

    private static final Long BYTES_LIMIT_FALLBACK = 10737418240L;
    private static final Long MESSAGES_LIMIT_FALLBACK = 10000000L;
    private static final Long BATCHES_LIMIT_FALLBACK = 1000000L;
    private final long windowLengthMs;
    private final int sampleCount;

    private final ConcurrentMap<ThrottleKey, ThrottleMetrics> metrics = new ConcurrentHashMap<>();
    private final ZkConfigurationService zkConfigurationService;

    @Autowired
    public ThrottlingService(@Value("${nakadi.throttling.windowLengthMs}") final long windowLengthMs,
                             @Value("${nakadi.throttling.sampleCount}") final int sampleCount,
                             final ZkConfigurationService zkConfigurationService) {
        this.windowLengthMs = windowLengthMs;
        this.sampleCount = sampleCount;
        this.zkConfigurationService = zkConfigurationService;
    }

    public ThrottleResult mark(final String application, final String eventType, final int size,
                               final int messagesCount) {
        final long now = Instant.now().getMillis();
        final ThrottleMetrics throttleMetrics = metricsFor(application, eventType);

        final ThrottleResult result = getThrottleResult(throttleMetrics, size, messagesCount, now);
        if (!result.isThrottled()) {
            throttleMetrics.mark(size, messagesCount, now);
            return result;
        }
        return result;
    }

    private ThrottleResult getThrottleResult(final ThrottleMetrics metrics, final long size, final long messagesCount,
                                             final long now) {
        //return metrics for the 15 last minutes
        final long batches = metrics.getBatchCount().measure(now);
        final long bytes = metrics.getByteCount().measure(now);
        final long messages = metrics.getMessageCount().measure(now);

        final long bytesLimit = zkConfigurationService.getLong(BYTES_LIMIT, BYTES_LIMIT_FALLBACK);
        final long messagesLimit = zkConfigurationService.getLong(MESSAGES_LIMIT, MESSAGES_LIMIT_FALLBACK);
        final long batchesLimit = zkConfigurationService.getLong(BATCHES_LIMIT, BATCHES_LIMIT_FALLBACK);

        final long bytesRemaining = bytesLimit - bytes;
        final long messagesRemaining = messagesLimit - messages;
        final long batchesRemaining = batchesLimit - batches;

        final long bytesAfter = bytesRemaining - size;
        final long messagesAfter = messagesRemaining - messagesCount;
        final long batchesAfter = batchesRemaining - 1;

        final boolean throttled = batchesAfter < 0 || bytesAfter < 0 || messagesAfter < 0;

        final Instant reset = Instant.now().plus(windowLengthMs);
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
                key -> new ThrottleMetrics(windowLengthMs, sampleCount));
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
