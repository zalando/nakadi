package de.zalando.aruha.nakadi.service;

import javax.annotation.Nullable;
import java.util.Optional;

import static java.util.Optional.ofNullable;

public class EventStreamConfig {
    private String topic;
    private Integer batchLimit;
    private Optional<Integer> streamLimit;
    private Optional<Integer> batchTimeout;
    private Optional<Integer> streamTimeout;
    private Optional<Integer> batchKeepAliveLimit;

    public EventStreamConfig(final String topic, final Integer batchLimit,
            final Optional<Integer> streamLimit, final Optional<Integer> batchTimeout,
            final Optional<Integer> streamTimeout, final Optional<Integer> batchKeepAliveLimit) {
        this.topic = topic;
        this.batchLimit = batchLimit;
        this.streamLimit = streamLimit;
        this.batchTimeout = batchTimeout;
        this.streamTimeout = streamTimeout;
        this.batchKeepAliveLimit = batchKeepAliveLimit;
    }

    public String getTopic() {
        return topic;
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
        return "EventStreamConfig{" + "topic='" + topic + '\'' + ", batchLimit=" + batchLimit
                + ", streamLimit=" + streamLimit + ", batchTimeout=" + batchTimeout + ", streamTimeout=" + streamTimeout
                + ", batchKeepAliveLimit=" + batchKeepAliveLimit + '}';
    }

    public static class Builder {
        private String topic;
        private Integer batchLimit;
        private Optional<Integer> streamLimit = Optional.empty();
        private Optional<Integer> batchTimeout = Optional.empty();
        private Optional<Integer> streamTimeout = Optional.empty();
        private Optional<Integer> batchKeepAliveLimit = Optional.empty();

        private Builder() { }

        public static Builder anEventStreamConfig() {
            return new Builder();
        }

        public Builder withTopic(final String topic) {
            this.topic = topic;
            return this;
        }

        public Builder withBatchLimit(final Integer batchLimit) {
            this.batchLimit = batchLimit;
            return this;
        }

        public Builder withStreamLimit(@Nullable final Integer streamLimit) {
            this.streamLimit = ofNullable(streamLimit);
            return this;
        }

        public Builder withBatchTimeout(@Nullable final Integer batchTimeout) {
            this.batchTimeout = ofNullable(batchTimeout);
            return this;
        }

        public Builder withStreamTimeout(@Nullable final Integer streamTimeout) {
            this.streamTimeout = ofNullable(streamTimeout);
            return this;
        }

        public Builder withBatchKeepAliveLimit(@Nullable final Integer batchKeepAliveLimit) {
            this.batchKeepAliveLimit = ofNullable(batchKeepAliveLimit);
            return this;
        }

        public EventStreamConfig build() {
            return new EventStreamConfig(topic, batchLimit, streamLimit, batchTimeout, streamTimeout,
                    batchKeepAliveLimit);
        }
    }
}
