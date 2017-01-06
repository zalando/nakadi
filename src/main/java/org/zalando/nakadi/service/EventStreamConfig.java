package org.zalando.nakadi.service;

import java.util.List;
import org.zalando.nakadi.domain.TopicPosition;
import org.zalando.nakadi.exceptions.UnprocessableEntityException;

import javax.annotation.Nullable;
import javax.annotation.concurrent.Immutable;

@Immutable
public class EventStreamConfig {

    private static final int BATCH_LIMIT_DEFAULT = 1;
    private static final int STREAM_LIMIT_DEFAULT = 0;
    private static final int BATCH_FLUSH_TIMEOUT_DEFAULT = 30;
    private static final int STREAM_TIMEOUT_DEFAULT = 0;
    private static final int STREAM_KEEP_ALIVE_LIMIT_DEFAULT = 0;

    private final List<TopicPosition> cursors;
    private final int batchLimit;
    private final int streamLimit;
    private final int batchTimeout;
    private final int streamTimeout;
    private final int streamKeepAliveLimit;
    private final String etName;
    private final String consumingAppId;

    private EventStreamConfig(final List<TopicPosition> cursors, final int batchLimit,
                             final int streamLimit, final int batchTimeout, final int streamTimeout,
                             final int streamKeepAliveLimit, final String etName, final String consumingAppId) {
        this.cursors = cursors;
        this.batchLimit = batchLimit;
        this.streamLimit = streamLimit;
        this.batchTimeout = batchTimeout;
        this.streamTimeout = streamTimeout;
        this.streamKeepAliveLimit = streamKeepAliveLimit;
        this.etName = etName;
        this.consumingAppId = consumingAppId;
    }

    public List<TopicPosition> getCursors() {
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

    public String getEtName() {
        return etName;
    }

    public String getConsumingAppId() {
        return consumingAppId;
    }

    @Override
    public String toString() {
        return "EventStreamConfig{cursors=" + cursors + ", batchLimit=" + batchLimit
                + ", streamLimit=" + streamLimit + ", batchTimeout=" + batchTimeout + ", streamTimeout=" + streamTimeout
                + ", streamKeepAliveLimit=" + streamKeepAliveLimit + '}';
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        final EventStreamConfig that = (EventStreamConfig) o;

        return batchLimit == that.batchLimit && streamLimit == that.streamLimit && batchTimeout == that.batchTimeout
                && streamTimeout == that.streamTimeout && streamKeepAliveLimit == that.streamKeepAliveLimit
                && cursors.equals(that.cursors);
    }

    @Override
    public int hashCode() {
        int result = cursors.hashCode();
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

        private List<TopicPosition> cursors = null;
        private int batchLimit = BATCH_LIMIT_DEFAULT;
        private int streamLimit = STREAM_LIMIT_DEFAULT;
        private int batchTimeout = BATCH_FLUSH_TIMEOUT_DEFAULT;
        private int streamTimeout = STREAM_TIMEOUT_DEFAULT;
        private int streamKeepAliveLimit = STREAM_KEEP_ALIVE_LIMIT_DEFAULT;
        private String etName;
        private String consumingAppId;

        public Builder withCursors(final List<TopicPosition> cursors) {
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

        public Builder withEtName(final String etName) {
            this.etName = etName;
            return this;
        }

        public Builder withConsumingAppId(final String consumingAppId) {
            this.consumingAppId = consumingAppId;
            return this;
        }


        public EventStreamConfig build() throws UnprocessableEntityException {
            if (streamLimit != 0 && streamLimit < batchLimit) {
                throw new UnprocessableEntityException("stream_limit can't be lower than batch_limit");
            } else if (streamTimeout != 0 && streamTimeout < batchTimeout) {
                throw new UnprocessableEntityException("stream_timeout can't be lower than batch_flush_timeout");
            }
            return new EventStreamConfig(cursors, batchLimit, streamLimit, batchTimeout, streamTimeout,
                    streamKeepAliveLimit, etName, consumingAppId);
        }
    }

}
