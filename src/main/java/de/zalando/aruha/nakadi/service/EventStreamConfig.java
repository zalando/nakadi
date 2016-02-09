package de.zalando.aruha.nakadi.service;

import com.google.common.collect.ImmutableMap;

import javax.annotation.concurrent.Immutable;
import java.util.Map;

@Immutable
public class EventStreamConfig {
    private final String topic;
    private final Map<String, String> cursors;
    private final int batchLimit;
    private final int streamLimit;
    private final int batchTimeout;
    private final int streamTimeout;
    private final int streamKeepAliveLimit;

    public EventStreamConfig(final String topic, final Map<String, String> cursors, final int batchLimit,
                             final int streamLimit, final int batchTimeout, final int streamTimeout,
                             final int streamKeepAliveLimit) {
        this.topic = topic;
        this.cursors = cursors;
        this.batchLimit = batchLimit;
        this.streamLimit = streamLimit;
        this.batchTimeout = batchTimeout;
        this.streamTimeout = streamTimeout;
        this.streamKeepAliveLimit = streamKeepAliveLimit;
    }

    public String getTopic() {
        return topic;
    }

    public Map<String, String> getCursors() {
        return cursors;
    }

    public int getBatchLimit() {
        return batchLimit;
    }

    public int getStreamLimit() {
        return streamLimit;
    }

    public int getBatchTimeout() {
        return batchTimeout;
    }

    public int getStreamTimeout() {
        return streamTimeout;
    }

    public int getStreamKeepAliveLimit() {
        return streamKeepAliveLimit;
    }

    @Override
    public String toString() {
        return "EventStreamConfig{" + "topic='" + topic + '\'' + ", cursors=" + cursors + ", batchLimit=" + batchLimit
                + ", streamLimit=" + streamLimit + ", batchTimeout=" + batchTimeout + ", streamTimeout=" + streamTimeout
                + ", streamKeepAliveLimit=" + streamKeepAliveLimit + '}';
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        final EventStreamConfig that = (EventStreamConfig) o;

        if (batchLimit != that.batchLimit) return false;
        if (streamLimit != that.streamLimit) return false;
        if (batchTimeout != that.batchTimeout) return false;
        if (streamTimeout != that.streamTimeout) return false;
        if (streamKeepAliveLimit != that.streamKeepAliveLimit) return false;
        if (!topic.equals(that.topic)) return false;
        return cursors.equals(that.cursors);

    }

    @Override
    public int hashCode() {
        int result = topic.hashCode();
        result = 31 * result + cursors.hashCode();
        result = 31 * result + batchLimit;
        result = 31 * result + streamLimit;
        result = 31 * result + batchTimeout;
        result = 31 * result + streamTimeout;
        result = 31 * result + streamKeepAliveLimit;
        return result;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {

        private String topic = null;
        private Map<String, String> cursors = ImmutableMap.of();
        private int batchLimit = 0;
        private int streamLimit = 0;
        private int batchTimeout = 0;
        private int streamTimeout = 0;
        private int streamKeepAliveLimit = 0;

        public Builder withTopic(final String topic) {
            this.topic = topic;
            return this;
        }

        public Builder withCursors(final Map<String, String> cursors) {
            this.cursors = cursors;
            return this;
        }

        public Builder withBatchLimit(final int batchLimit) {
            this.batchLimit = batchLimit;
            return this;
        }

        public Builder withStreamLimit(final int streamLimit) {
            this.streamLimit = streamLimit;
            return this;
        }

        public Builder withBatchTimeout(final int batchTimeout) {
            this.batchTimeout = batchTimeout;
            return this;
        }

        public Builder withStreamTimeout(final int streamTimeout) {
            this.streamTimeout = streamTimeout;
            return this;
        }

        public Builder withStreamKeepAliveLimit(final int streamKeepAliveLimit) {
            this.streamKeepAliveLimit = streamKeepAliveLimit;
            return this;
        }

        public EventStreamConfig build() {
            if (topic == null) {
                throw new IllegalStateException("Topic should be specified");
            }
            if (batchLimit == 0) {
                throw new IllegalStateException("Batch limit should be specified");
            }
            return new EventStreamConfig(topic, cursors, batchLimit, streamLimit, batchTimeout, streamTimeout, streamKeepAliveLimit);
        }
    }

}
