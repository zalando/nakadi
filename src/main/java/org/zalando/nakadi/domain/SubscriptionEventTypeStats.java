package org.zalando.nakadi.domain;

import com.fasterxml.jackson.annotation.JsonInclude;
import java.util.Collections;
import java.util.Objects;
import java.util.Set;
import javax.annotation.Nullable;
import javax.annotation.concurrent.Immutable;

@Immutable
public class SubscriptionEventTypeStats {

    private final String eventType;
    private final Set<Partition> partitions;

    public SubscriptionEventTypeStats(final String eventType, final Set<Partition> partitions) {
        this.eventType = eventType;
        this.partitions = partitions;
    }

    public String getEventType() {
        return eventType;
    }

    public Set<Partition> getPartitions() {
        return Collections.unmodifiableSet(partitions);
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof SubscriptionEventTypeStats)) {
            return false;
        }

        final SubscriptionEventTypeStats that = (SubscriptionEventTypeStats) o;

        return Objects.equals(eventType, that.eventType) && Objects.equals(partitions, that.partitions);
    }

    @Override
    public int hashCode() {
        return eventType != null ? eventType.hashCode() : 0;
    }

    @Immutable
    public static class Partition {
        private final String partition;
        private final String state;
        @JsonInclude(JsonInclude.Include.NON_NULL)
        private final Long unconsumedEvents;
        private final String streamId;

        public Partition(final String partition,
                         final String state,
                         @Nullable final Long unconsumedEvents,
                         final String streamId) {
            this.partition = partition;
            this.state = state;
            this.unconsumedEvents = unconsumedEvents;
            this.streamId = streamId;
        }

        public String getPartition() {
            return partition;
        }

        public String getState() {
            return state;
        }

        @Nullable
        public Long getUnconsumedEvents() {
            return unconsumedEvents;
        }

        public String getStreamId() {
            return streamId;
        }

        @Override
        public boolean equals(final Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof Partition)) {
                return false;
            }

            final Partition that = (Partition) o;

            return Objects.equals(partition, that.partition)
                    && Objects.equals(state, that.state)
                    && Objects.equals(unconsumedEvents, that.unconsumedEvents)
                    && Objects.equals(streamId, that.streamId);
        }

        @Override
        public int hashCode() {
            return partition != null ? partition.hashCode() : 0;
        }
    }
}
