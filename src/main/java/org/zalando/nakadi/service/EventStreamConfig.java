package org.zalando.nakadi.service;

import org.zalando.nakadi.domain.NakadiCursor;
import org.zalando.nakadi.exceptions.runtime.InvalidLimitException;
import org.zalando.nakadi.security.Client;

import javax.annotation.Nullable;
import javax.annotation.concurrent.Immutable;
import java.util.List;
import java.util.Random;

@Immutable
public class EventStreamConfig {

    public static final int MAX_STREAM_TIMEOUT = 3600 + 600; // 1h 10m

    private static final int BATCH_LIMIT_DEFAULT = 1;
    private static final int STREAM_LIMIT_DEFAULT = 0;
    private static final int BATCH_FLUSH_TIMEOUT_DEFAULT = 30;
    private static final int STREAM_KEEP_ALIVE_LIMIT_DEFAULT = 0;
    private static final long DEF_MAX_MEMORY_USAGE_BYTES = 50 * 1024 * 1024;
    private static final Random RANDOM = new Random();

    private final List<NakadiCursor> cursors;
    private final int batchLimit;
    private final int streamLimit;
    private final int batchTimeout;
    private final int streamTimeout;
    private final int streamKeepAliveLimit;
    private final String etName;
    private final Client consumingClient;
    private final long maxMemoryUsageBytes;

    private EventStreamConfig(final List<NakadiCursor> cursors, final int batchLimit,
                              final int streamLimit, final int batchTimeout, final int streamTimeout,
                              final int streamKeepAliveLimit, final String etName, final Client consumingClient,
                              final long maxMemoryUsageBytes) {
        this.cursors = cursors;
        this.batchLimit = batchLimit;
        this.streamLimit = streamLimit;
        this.batchTimeout = batchTimeout;
        this.streamTimeout = streamTimeout;
        this.streamKeepAliveLimit = streamKeepAliveLimit;
        this.etName = etName;
        this.consumingClient= consumingClient;
        this.maxMemoryUsageBytes = maxMemoryUsageBytes;
    }

    public List<NakadiCursor> getCursors() {
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

    public Client getConsumingClient() {
        return consumingClient;
    }

    public long getMaxMemoryUsageBytes() {
        return maxMemoryUsageBytes;
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

    public static int generateDefaultStreamTimeout() {
        return 3600 + RANDOM.nextInt(1200) - 600; // 1h ± 10min
    }

    public static class Builder {

        private List<NakadiCursor> cursors = null;
        private int batchLimit = BATCH_LIMIT_DEFAULT;
        private int streamLimit = STREAM_LIMIT_DEFAULT;
        private int batchTimeout = BATCH_FLUSH_TIMEOUT_DEFAULT;
        private int streamTimeout = generateDefaultStreamTimeout();
        private int streamKeepAliveLimit = STREAM_KEEP_ALIVE_LIMIT_DEFAULT;
        private long maxMemoryUsageBytes = DEF_MAX_MEMORY_USAGE_BYTES;
        private String etName;
        private Client consumingClient;

        public Builder withCursors(final List<NakadiCursor> cursors) {
            this.cursors = cursors;
            return this;
        }

        public Builder withMaxMemoryUsageBytes(@Nullable final Long maxMemoryUsageBytes) {
            if (null != maxMemoryUsageBytes) {
                this.maxMemoryUsageBytes = maxMemoryUsageBytes;
            }
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
            if (streamTimeout != null && streamTimeout <= MAX_STREAM_TIMEOUT && streamTimeout > 0) {
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

        public Builder withConsumingClient(final Client consumingClient) {
            this.consumingClient = consumingClient;
            return this;
        }


        public EventStreamConfig build() throws InvalidLimitException {
            if (streamLimit != 0 && streamLimit < batchLimit) {
                throw new InvalidLimitException("stream_limit can't be lower than batch_limit");
            } else if (streamTimeout != 0 && streamTimeout < batchTimeout) {
                throw new InvalidLimitException("stream_timeout can't be lower than batch_flush_timeout");
            } else if (batchLimit < 1) {
                throw new InvalidLimitException("batch_limit can't be lower than 1");
            }
            return new EventStreamConfig(cursors, batchLimit, streamLimit, batchTimeout, streamTimeout,
                    streamKeepAliveLimit, etName, consumingClient, maxMemoryUsageBytes);
        }
    }

}
