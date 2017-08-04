package org.zalando.nakadi.domain;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

import javax.annotation.Nullable;
import javax.annotation.concurrent.Immutable;
import java.util.Collections;
import java.util.List;

@Immutable
public class SubscriptionEventTypeStats {

    private final String eventType;
    private final List<Partition> partitions;

    public SubscriptionEventTypeStats(
            @JsonProperty("event_type") final String eventType,
            @JsonProperty("partitions") final List<Partition> partitions) {
        this.eventType = eventType;
        this.partitions = partitions;
    }

    public String getEventType() {
        return eventType;
    }

    public List<Partition> getPartitions() {
        return Collections.unmodifiableList(partitions);
    }

    @Immutable
    public static class Partition {
        private final String partition;
        private final String state;
        @JsonInclude(JsonInclude.Include.NON_NULL)
        private final Long unconsumedEvents;
        private final String streamId;

        public Partition(
                @JsonProperty("partition") final String partition,
                @JsonProperty("state") final String state,
                @JsonProperty("unconsumed_events") @Nullable final Long unconsumedEvents,
                @JsonProperty("stream_id") final String streamId) {
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
