package de.zalando.aruha.nakadi.service;

import java.util.Map;
import java.util.Optional;

public class EventStreamConfig {
    private String topic;
    private Map<String, String> cursors;
    private Integer batchLimit;
    private Optional<Integer> streamLimit;
    private Optional<Integer> batchTimeout;
    private Optional<Integer> streamTimeout;
    private Optional<Integer> batchKeepAliveLimit;

    public EventStreamConfig(final String topic,
                             final Map<String, String> cursors,
                             final Integer batchLimit,
                             final Optional<Integer> streamLimit,
                             final Optional<Integer> batchTimeout,
                             final Optional<Integer> streamTimeout,
                             final Optional<Integer> batchKeepAliveLimit) {
        this.topic = topic;
        this.cursors = cursors;
        this.batchLimit = batchLimit;
        this.streamLimit = streamLimit;
        this.batchTimeout = batchTimeout;
        this.streamTimeout = streamTimeout;
        this.batchKeepAliveLimit = batchKeepAliveLimit;
    }

    public String getTopic() {
        return topic;
    }

    public Map<String, String> getCursors() {
        return cursors;
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
