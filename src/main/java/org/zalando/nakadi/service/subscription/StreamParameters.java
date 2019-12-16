package org.zalando.nakadi.service.subscription;

import org.zalando.nakadi.domain.EventTypePartition;
import org.zalando.nakadi.exceptions.runtime.WrongStreamParametersException;
import org.zalando.nakadi.security.Client;
import org.zalando.nakadi.service.EventStreamConfig;
import org.zalando.nakadi.view.UserStreamParameters;

import java.util.List;
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
     * Number of milliseconds to be batched based on Kafka record timestamp.
     */
    public final long batchTimespan;
    /**
     * Timeout for collecting {@code batchLimitEvents} events. If not collected - send either not full batch
     * or keep alive message.
     */
    public final long batchTimeoutMillis;
    /**
     * Stream time to live
     */
    public final long streamTimeoutMillis;
    /**
     * If count of keepAliveIterations in a row for each batch is reached - stream is closed.
     * Works only if set.
     */
    private final Optional<Integer> batchKeepAliveIterations;

    // Applies to stream, number of messages to send to clients
    public final int maxUncommittedMessages;

    // Applies to stream. Timeout without commits.
    public final long commitTimeoutMillis;

    private final Client consumingClient;

    private final List<EventTypePartition> partitions;

    private StreamParameters(
            final UserStreamParameters userParameters,
            final long maxCommitTimeout,
            final Client consumingClient) throws WrongStreamParametersException {

        this.batchLimitEvents = userParameters.getBatchLimit().orElse(1);
        if (batchLimitEvents <= 0) {
            throw new WrongStreamParametersException("batch_limit can't be lower than 1");
        }
        this.streamLimitEvents = userParameters.getStreamLimit().filter(v -> v != 0);
        this.batchTimespan = TimeUnit.SECONDS.toMillis(userParameters.getBatchTimespan().orElse(0L));
        this.batchTimeoutMillis = TimeUnit.SECONDS.toMillis(userParameters.getBatchFlushTimeout().orElse(30));
        this.streamTimeoutMillis = TimeUnit.SECONDS.toMillis(
                userParameters.getStreamTimeout()
                        .filter(timeout -> timeout > 0 && timeout <= EventStreamConfig.MAX_STREAM_TIMEOUT)
                        .orElse((long) EventStreamConfig.generateDefaultStreamTimeout()));
        this.maxUncommittedMessages = userParameters.getMaxUncommittedEvents().orElse(10);
        this.batchKeepAliveIterations = userParameters.getStreamKeepAliveLimit().filter(v -> v != 0);
        this.partitions = userParameters.getPartitions();
        this.consumingClient = consumingClient;

        final long commitTimeout = userParameters.getCommitTimeoutSeconds().orElse(maxCommitTimeout);
        if (commitTimeout > maxCommitTimeout) {
            throw new WrongStreamParametersException("commit_timeout can not be more than " + maxCommitTimeout);
        } else if (commitTimeout < 0) {
            throw new WrongStreamParametersException("commit_timeout can not be less than 0");
        }
        this.commitTimeoutMillis = TimeUnit.SECONDS.toMillis(commitTimeout == 0 ? maxCommitTimeout : commitTimeout);
    }

    public long getMessagesAllowedToSend(final long limit, final long sentSoFar) {
        return streamLimitEvents.map(v -> Math.max(0, Math.min(limit, v - sentSoFar))).orElse(limit);
    }

    public boolean isStreamLimitReached(final long committedEvents) {
        return streamLimitEvents.map(v -> v <= committedEvents).orElse(false);
    }

    public boolean isKeepAliveLimitReached(final IntStream keepAlive) {
        return batchKeepAliveIterations.map(it -> keepAlive.allMatch(v -> v >= it)).orElse(false);
    }

    public Client getConsumingClient() {
        return consumingClient;
    }

    public List<EventTypePartition> getPartitions() {
        return partitions;
    }

    public static StreamParameters of(final UserStreamParameters userStreamParameters,
                                      final long maxCommitTimeoutSeconds,
                                      final Client client) throws WrongStreamParametersException {
        return new StreamParameters(userStreamParameters, maxCommitTimeoutSeconds, client);
    }

}
