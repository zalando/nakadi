package org.zalando.nakadi.domain;

import com.fasterxml.jackson.annotation.JsonInclude;

import javax.annotation.Nullable;
import javax.annotation.concurrent.Immutable;
import java.util.Collections;
import java.util.Set;

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
    }
}
