package de.zalando.aruha.nakadi.service.subscription;

import java.util.Optional;
import java.util.concurrent.TimeUnit;

public class StreamParameters {
    /**
     * Maximum amount of events in batch. If it's reached, batch is sent immediately and stream is flushed.
     */
    public final int batchLimitEvents;
    /**
     * Maximum number of events that could be sent in session
     */
    public final Integer streamLimitEvents;
    /**
     * Timeout for collecting {@code batchLimitEvents} events. If not collected - send either not full batch
     * or keep alive message.
     */
    public final long batchTimeoutMillis;
    /**
     * Stream time to live
     */
    public final Long streamTimeoutMillis;
    /**
     * If count of keepAliveIterations in a row for each batch is reached - stream is closed.
     * Works only if set.
     */
    public final Integer batchKeepAliveIterations;

    // Applies to stream, number of messages to send to clients
    public final int windowSizeMessages;

    // Applies to stream. Timeout without commits.
    public final long commitTimeoutMillis;

    private StreamParameters(
            final int batchLimitEvents, final Integer streamLimitEvents, final long batchTimeoutMillis,
            final Long streamTimeoutMillis, final Integer batchKeepAliveIterations, final int windowSizeMessages,
            final long commitTimeoutMillis) {
        this.batchLimitEvents = batchLimitEvents;
        this.streamLimitEvents = streamLimitEvents;
        this.batchTimeoutMillis = batchTimeoutMillis;
        this.streamTimeoutMillis = streamTimeoutMillis;
        this.batchKeepAliveIterations = batchKeepAliveIterations;
        this.windowSizeMessages = windowSizeMessages;
        this.commitTimeoutMillis = commitTimeoutMillis;
    }

    public static StreamParameters of(
            final int batchLimitEvents,
            final Integer streamLimitEvents,
            final long batchTimeoutSeconds,
            final Long streamTimeoutSeconds,
            final Integer batchKeepAliveIterations,
            final int windowSizeMessages,
            final long commitTimeoutSeconds) {
        return new StreamParameters(
                batchLimitEvents,
                streamLimitEvents,
                TimeUnit.SECONDS.toMillis(batchTimeoutSeconds),
                Optional.ofNullable(streamTimeoutSeconds).map(TimeUnit.SECONDS::toMillis).orElse(null),
                batchKeepAliveIterations,
                windowSizeMessages,
                TimeUnit.SECONDS.toMillis(commitTimeoutSeconds));
    }
}
