package de.zalando.aruha.nakadi.service;

import java.util.Optional;

public class EventStreamConfig {
    private String topicId;
    private String partitionId;
    private String startFrom;
    private Integer batchLimit;
    private Optional<Integer> streamLimit;
    private Optional<Integer> batchTimeout;
    private Optional<Integer> streamTimeout;
    private Optional<Integer> batchKeepAliveLimit;

    public EventStreamConfig(final String topicId,
                             final String partitionId,
                             final String startFrom,
                             final Integer batchLimit,
                             final Optional<Integer> streamLimit,
                             final Optional<Integer> batchTimeout,
                             final Optional<Integer> streamTimeout,
                             final Optional<Integer> batchKeepAliveLimit) {
        this.topicId = topicId;
        this.partitionId = partitionId;
        this.startFrom = startFrom;
        this.batchLimit = batchLimit;
        this.streamLimit = streamLimit;
        this.batchTimeout = batchTimeout;
        this.streamTimeout = streamTimeout;
        this.batchKeepAliveLimit = batchKeepAliveLimit;
    }

    public String getTopicId() {
        return topicId;
    }

    public String getPartitionId() {
        return partitionId;
    }

    public String getStartFrom() {
        return startFrom;
    }

    public Integer getBatchLimit() {
        return batchLimit;
    }

    public Optional<Integer> getStreamLimit() {
        return streamLimit;
    }

    public Optional<Integer> getBatchTimeout() {
        return batchTimeout;
    }

    public Optional<Integer> getStreamTimeout() {
        return streamTimeout;
    }

    public Optional<Integer> getBatchKeepAliveLimit() {
        return batchKeepAliveLimit;
    }
}
