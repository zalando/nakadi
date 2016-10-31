package org.zalando.nakadi.throttling;

import org.joda.time.Instant;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.zalando.nakadi.util.ZkConfigurationService;

import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;

@Component
public class ThrottlingService {

    private static final String BYTES_LIMIT = "nakadi.throttling.bytesLimit";

    private static final Long BYTES_LIMIT_FALLBACK = 1073741824L;
    private final long windowLengthMs;
    private final int sampleCount;

    private final ConcurrentMap<ThrottleKey, SampledTotal> metrics = new ConcurrentHashMap<>();
    private final ZkConfigurationService zkConfigurationService;

    @Autowired
    public ThrottlingService(@Value("${nakadi.throttling.windowLengthMs}") final long windowLengthMs,
                             @Value("${nakadi.throttling.sampleCount}") final int sampleCount,
                             final ZkConfigurationService zkConfigurationService) {
        this.windowLengthMs = windowLengthMs;
        this.sampleCount = sampleCount;
        this.zkConfigurationService = zkConfigurationService;
    }

    public ThrottleResult mark(final String application, final String eventType, final long size) {
        final long now = Instant.now().getMillis();
        final SampledTotal sampledTotal = metricsFor(application, eventType);

        final ThrottleResult result = getThrottleResult(sampledTotal, size, now);
        if (!result.isThrottled()) {
            sampledTotal.record(size, now);
            return result;
        }
        return result;
    }

    private ThrottleResult getThrottleResult(final SampledTotal metrics, final long size, final long now) {
        final long bytes = metrics.measure(now);
        final long bytesLimit = zkConfigurationService.getLong(BYTES_LIMIT, BYTES_LIMIT_FALLBACK);
        final long bytesRemaining = bytesLimit - bytes;
        final long bytesAfter = bytesRemaining - size;
        final boolean throttled = bytesAfter < 0;

        final Instant reset = Instant.now().plus(windowLengthMs);
        if (throttled) {
            return new ThrottleResult(bytesLimit, remaining(bytesRemaining), reset, true);
        }
        return new ThrottleResult(bytesLimit, bytesAfter, reset, false);
    }

    private long remaining(final long value) {
        return value <= 0 ? 0 : value;
    }

    private SampledTotal metricsFor(final String application, final String eventType) {
        return metrics.computeIfAbsent(ThrottleKey.key(application, eventType),
                key -> new SampledTotal(windowLengthMs, TimeUnit.MILLISECONDS, sampleCount));
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
