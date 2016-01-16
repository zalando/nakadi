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

  public EventStreamConfig(
      final String topic,
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

  public static Builder builder() {
    return Builder.anEventStreamConfig();
  }

  @Override
  public String toString() {
    return "EventStreamConfig{"
        + "topic='"
        + topic
        + '\''
        + ", cursors="
        + cursors
        + ", batchLimit="
        + batchLimit
        + ", streamLimit="
        + streamLimit
        + ", batchTimeout="
        + batchTimeout
        + ", streamTimeout="
        + streamTimeout
        + ", batchKeepAliveLimit="
        + batchKeepAliveLimit
        + '}';
  }

  public static class Builder {
    private String topic;
    private Map<String, String> cursors;
    private Integer batchLimit;
    private Optional<Integer> streamLimit;
    private Optional<Integer> batchTimeout;
    private Optional<Integer> streamTimeout;
    private Optional<Integer> batchKeepAliveLimit;

    private Builder() {}

    public static Builder anEventStreamConfig() {
      return new Builder();
    }

    public Builder withTopic(final String topic) {
      this.topic = topic;
      return this;
    }

    public Builder withCursors(final Map<String, String> cursors) {
      this.cursors = cursors;
      return this;
    }

    public Builder withBatchLimit(final Integer batchLimit) {
      this.batchLimit = batchLimit;
      return this;
    }

    public Builder withStreamLimit(final Optional<Integer> streamLimit) {
      this.streamLimit = streamLimit;
      return this;
    }

    public Builder withBatchTimeout(final Optional<Integer> batchTimeout) {
      this.batchTimeout = batchTimeout;
      return this;
    }

    public Builder withStreamTimeout(final Optional<Integer> streamTimeout) {
      this.streamTimeout = streamTimeout;
      return this;
    }

    public Builder withBatchKeepAliveLimit(final Optional<Integer> batchKeepAliveLimit) {
      this.batchKeepAliveLimit = batchKeepAliveLimit;
      return this;
    }

    public EventStreamConfig build() {
      return new EventStreamConfig(
          topic,
          cursors,
          batchLimit,
          streamLimit,
          batchTimeout,
          streamTimeout,
          batchKeepAliveLimit);
    }
  }
}
