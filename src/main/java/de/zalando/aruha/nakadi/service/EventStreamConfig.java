package de.zalando.aruha.nakadi.service;

import com.google.common.collect.ImmutableMap;
import de.zalando.aruha.nakadi.exceptions.UnprocessableEntityException;

import javax.annotation.Nullable;
import javax.annotation.concurrent.Immutable;
import java.util.Map;

@Immutable
public class EventStreamConfig {

    private static final int BATCH_LIMIT_DEFAULT = 1;
    private static final int STREAM_LIMIT_DEFAULT = 0;
    private static final int BATCH_FLUSH_TIMEOUT_DEFAULT = 30;
    private static final int STREAM_TIMEOUT_DEFAULT = 0;
    private static final int STREAM_KEEP_ALIVE_LIMIT_DEFAULT = 0;

    private final String topic;
    private final Map<String, String> cursors;
    private final int batchLimit;
    private final int streamLimit;
    private final int batchTimeout;
    private final int streamTimeout;
    private final int streamKeepAliveLimit;

    private EventStreamConfig(final String topic, final Map<String, String> cursors, final int batchLimit,
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
        private int batchLimit = BATCH_LIMIT_DEFAULT;
        private int streamLimit = STREAM_LIMIT_DEFAULT;
        private int batchTimeout = BATCH_FLUSH_TIMEOUT_DEFAULT;
        private int streamTimeout = STREAM_TIMEOUT_DEFAULT;
        private int streamKeepAliveLimit = STREAM_KEEP_ALIVE_LIMIT_DEFAULT;

        public Builder withTopic(final String topic) {
            this.topic = topic;
            return this;
        }

        public Builder withCursors(final Map<String, String> cursors) {
            this.cursors = cursors;
            return this;
        }

        public Builder withBatchLimit(@Nullable final Integer batchLimit) {
            if (batchLimit != null) {
                this.batchLimit = batchLimit;
            }
            return this;
        }

        public Builder withStreamLimit(@Nullable final Integer streamLimit) {
            if (streamLimit != null) {
                this.streamLimit = streamLimit;
            }
            return this;
        }

        public Builder withBatchTimeout(@Nullable final Integer batchTimeout) {
            if (batchTimeout != null) {
                if (batchTimeout == 0) {
                    this.batchTimeout = BATCH_FLUSH_TIMEOUT_DEFAULT;
                } else {
                    this.batchTimeout = batchTimeout;
                }
            }
            return this;
        }

        public Builder withStreamTimeout(@Nullable final Integer streamTimeout) {
            if (streamTimeout != null) {
                this.streamTimeout = streamTimeout;
            }
            return this;
        }

        public Builder withStreamKeepAliveLimit(@Nullable final Integer streamKeepAliveLimit) {
            if (streamKeepAliveLimit != null) {
                this.streamKeepAliveLimit = streamKeepAliveLimit;
            }
            return this;
        }

        public EventStreamConfig build() throws UnprocessableEntityException {
            if (topic == null) {
                throw new IllegalStateException("Topic should be specified");
            } else if (streamLimit != 0 && streamLimit < batchLimit) {
                throw new UnprocessableEntityException("stream_limit can't be lower than batch_limit");
            } else if (streamTimeout != 0 && streamTimeout < batchTimeout) {
                throw new UnprocessableEntityException("stream_timeout can't be lower than batch_flush_timeout");
            }
            return new EventStreamConfig(topic, cursors, batchLimit, streamLimit, batchTimeout, streamTimeout,
                    streamKeepAliveLimit);
        }
    }

}
