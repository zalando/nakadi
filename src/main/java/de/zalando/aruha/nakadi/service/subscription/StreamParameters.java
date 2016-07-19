package de.zalando.aruha.nakadi.service.subscription;

import javax.annotation.Nullable;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

public class StreamParameters {
    /**
     * Maximum amount of events in batch. If it's reached, batch is sent immediately and stream is flushed.
     */
    public final int batchLimitEvents;
    /**
     * Maximum number of events that could be sent in session
     */
    private final Optional<Long> streamLimitEvents;
    /**
     * Timeout for collecting {@code batchLimitEvents} events. If not collected - send either not full batch
     * or keep alive message.
     */
    public final long batchTimeoutMillis;
    /**
     * Stream time to live
     */
    public final Optional<Long> streamTimeoutMillis;
    /**
     * If count of keepAliveIterations in a row for each batch is reached - stream is closed.
     * Works only if set.
     */
    private final Optional<Integer> batchKeepAliveIterations;

    // Applies to stream, number of messages to send to clients
    public final int windowSizeMessages;

    // Applies to stream. Timeout without commits.
    public final long commitTimeoutMillis;

    private StreamParameters(
            final int batchLimitEvents, @Nullable final Long streamLimitEvents, final long batchTimeoutMillis,
            @Nullable final Long streamTimeoutMillis, @Nullable final Integer batchKeepAliveIterations,
            final int windowSizeMessages, final long commitTimeoutMillis) {
        this.batchLimitEvents = batchLimitEvents;
        this.streamLimitEvents = Optional.ofNullable(streamLimitEvents);
        this.batchTimeoutMillis = batchTimeoutMillis;
        this.streamTimeoutMillis = Optional.ofNullable(streamTimeoutMillis);
        this.batchKeepAliveIterations = Optional.ofNullable(batchKeepAliveIterations);
        this.windowSizeMessages = windowSizeMessages;
        this.commitTimeoutMillis = commitTimeoutMillis;
    }

    public long getMessagesAllowedToSend(final long limit, final long sentSoFar) {
        return streamLimitEvents.map(v -> Math.max(0, Math.min(limit, v - sentSoFar))).orElse(limit);
    }

    public boolean isStreamLimitReached(final long commitedEvents) {
        return streamLimitEvents.map(v -> v <= commitedEvents).orElse(false);
    }

    public boolean isKeepAliveLimitReached(final IntStream keepAlive) {
        return batchKeepAliveIterations.map(it -> keepAlive.allMatch(v -> v >= it)).orElse(false);
    }

    public static StreamParameters of(
            final int batchLimitEvents,
            @Nullable final Long streamLimitEvents,
            final long batchTimeoutSeconds,
            @Nullable final Long streamTimeoutSeconds,
            @Nullable final Integer batchKeepAliveIterations,
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
