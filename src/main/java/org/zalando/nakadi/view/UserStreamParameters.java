package org.zalando.nakadi.view;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import org.zalando.nakadi.domain.EventTypePartition;

import javax.annotation.Nullable;
import javax.validation.Valid;
import java.util.List;
import java.util.Optional;

public class UserStreamParameters {

    private final Optional<Integer> batchLimit;

    private final Optional<Long> streamLimit;

    private final Optional<Integer> batchFlushTimeout;

    private final Optional<Long> streamTimeout;

    private final Optional<Integer> batchKeepAliveLimit;

    private final Optional<Integer> maxUncommittedEvent;

    @Valid
    private final List<EventTypePartition> partitions;

    @JsonCreator
    public UserStreamParameters(@JsonProperty("batch_limit") @Nullable final Integer batchLimit,
                                @JsonProperty("stream_limit") @Nullable final Long streamLimit,
                                @JsonProperty("batch_flush_timeout") @Nullable final Integer batchFlushTimeout,
                                @JsonProperty("stream_timeout") @Nullable final Long streamTimeout,
                                @JsonProperty("stream_keep_alive_limit") @Nullable final Integer streamKeepAliveLimit,
                                @JsonProperty("max_uncommitted_events") @Nullable final Integer maxUncommittedEvents,
                                @JsonProperty("partitions") @Nullable final List<EventTypePartition> partitions) {
        this.batchLimit = Optional.ofNullable(batchLimit);
        this.streamLimit = Optional.ofNullable(streamLimit);
        this.batchFlushTimeout = Optional.ofNullable(batchFlushTimeout);
        this.streamTimeout = Optional.ofNullable(streamTimeout);
        this.batchKeepAliveLimit = Optional.ofNullable(streamKeepAliveLimit);
        this.maxUncommittedEvent = Optional.ofNullable(maxUncommittedEvents);
        this.partitions = partitions == null ? ImmutableList.of() : partitions;
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

    public Optional<Integer> getBatchKeepAliveLimit() {
        return batchKeepAliveLimit;
    }

    public Optional<Integer> getMaxUncommittedEvent() {
        return maxUncommittedEvent;
    }

    public List<EventTypePartition> getPartitions() {
        return partitions;
    }
}
