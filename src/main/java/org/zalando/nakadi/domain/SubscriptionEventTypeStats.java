package org.zalando.nakadi.domain;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonValue;

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

        public enum AssignmentType {
            AUTO("auto"),
            DIRECT("direct");

            private final String description;

            AssignmentType(final String description) {
                this.description = description;
            }

            @JsonValue
            public String getDescription() {
                return description;
            }
        }

        private final String partition;

        private final String state;

        @JsonInclude(JsonInclude.Include.NON_NULL)
        private final Long unconsumedEvents;

        @JsonInclude(JsonInclude.Include.NON_NULL)
        private final Long consumerLagSeconds;

        private final String streamId;

        @JsonInclude(JsonInclude.Include.NON_NULL)
        private final AssignmentType assignmentType;

        public Partition(
                @JsonProperty("partition") final String partition,
                @JsonProperty("state") final String state,
                @JsonProperty("unconsumed_events") @Nullable final Long unconsumedEvents,
                @JsonProperty("consumer_lag_seconds") @Nullable final Long consumerLagSeconds,
                @JsonProperty("stream_id") final String streamId,
                @JsonProperty("assignment_type") @Nullable final AssignmentType assignmentType) {
            this.partition = partition;
            this.state = state;
            this.unconsumedEvents = unconsumedEvents;
            this.consumerLagSeconds = consumerLagSeconds;
            this.streamId = streamId;
            this.assignmentType = assignmentType;
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

        @Nullable
        public Long getConsumerLagSeconds() {
            return consumerLagSeconds;
        }

        public String getStreamId() {
            return streamId;
        }

        public AssignmentType getAssignmentType() {
            return assignmentType;
        }
    }
}
