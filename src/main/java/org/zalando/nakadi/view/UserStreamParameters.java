package org.zalando.nakadi.view;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import org.zalando.nakadi.domain.EventTypePartition;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Optional;

public class UserStreamParameters {

    private final Optional<Integer> batchLimit;

    private final Optional<Long> streamLimit;

    private final Optional<Integer> batchFlushTimeout;

    private final Optional<Long> streamTimeout;

    private final Optional<Integer> streamKeepAliveLimit;

    private final Optional<Integer> maxUncommittedEvents;

    private final List<EventTypePartition> partitions;

    private final Optional<Long> commitTimeoutSeconds;

    @JsonCreator
    public UserStreamParameters(@JsonProperty("batch_limit") @Nullable final Integer batchLimit,
                                @JsonProperty("stream_limit") @Nullable final Long streamLimit,
                                @JsonProperty("batch_flush_timeout") @Nullable final Integer batchFlushTimeout,
                                @JsonProperty("stream_timeout") @Nullable final Long streamTimeout,
                                @JsonProperty("stream_keep_alive_limit") @Nullable final Integer streamKeepAliveLimit,
                                @JsonProperty("max_uncommitted_events") @Nullable final Integer maxUncommittedEvents,
                                @JsonProperty("partitions") @Nullable final List<EventTypePartition> partitions,
                                @JsonProperty("commit_timeout") final Long commitTimeoutSeconds) {
        this.batchLimit = Optional.ofNullable(batchLimit);
        this.streamLimit = Optional.ofNullable(streamLimit);
        this.batchFlushTimeout = Optional.ofNullable(batchFlushTimeout);
        this.streamTimeout = Optional.ofNullable(streamTimeout);
        this.streamKeepAliveLimit = Optional.ofNullable(streamKeepAliveLimit);
        this.maxUncommittedEvents = Optional.ofNullable(maxUncommittedEvents);
        this.partitions = partitions == null ? ImmutableList.of() : partitions;
        this.commitTimeoutSeconds = Optional.ofNullable(commitTimeoutSeconds);
    }

    public Optional<Integer> getBatchLimit() {
        return batchLimit;
    }

    public Optional<Long> getStreamLimit() {
        return streamLimit;
    }

    public Optional<Integer> getBatchFlushTimeout() {
        return batchFlushTimeout;
    }

    public Optional<Long> getStreamTimeout() {
        return streamTimeout;
    }

    public Optional<Integer> getStreamKeepAliveLimit() {
        return streamKeepAliveLimit;
    }

    public Optional<Integer> getMaxUncommittedEvents() {
        return maxUncommittedEvents;
    }

    public List<EventTypePartition> getPartitions() {
        return partitions;
    }

    public Optional<Long> getCommitTimeoutSeconds() {
        return commitTimeoutSeconds;
    }
}
