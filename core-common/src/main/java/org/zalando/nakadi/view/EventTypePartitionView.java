package org.zalando.nakadi.view;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;

public class EventTypePartitionView {
    @JsonProperty("partition")
    private String partitionId;
    private String oldestAvailableOffset;
    private String newestAvailableOffset;
    @JsonProperty("unconsumed_events")
    @JsonInclude(JsonInclude.Include.NON_NULL)
    private Long unconsumedEvents;


    public EventTypePartitionView() {
    }

    public EventTypePartitionView(final String partitionId, final String oldestAvailableOffset,
                                  final String newestAvailableOffset) {
        this.partitionId = partitionId;
        this.oldestAvailableOffset = oldestAvailableOffset;
        this.newestAvailableOffset = newestAvailableOffset;
    }

    public EventTypePartitionView(final String partitionId, final String oldestAvailableOffset,
                                  final String newestAvailableOffset, final Long unconsumedEvents) {
        this.partitionId = partitionId;
        this.oldestAvailableOffset = oldestAvailableOffset;
        this.newestAvailableOffset = newestAvailableOffset;
        this.unconsumedEvents = unconsumedEvents;
    }

    public String getPartitionId() {
        return partitionId;
    }

    public void setPartitionId(final String partitionId) {
        this.partitionId = partitionId;
    }

    public String getOldestAvailableOffset() {
        return oldestAvailableOffset;
    }

    public void setOldestAvailableOffset(final String oldestAvailableOffset) {
        this.oldestAvailableOffset = oldestAvailableOffset;
    }

    public String getNewestAvailableOffset() {
        return newestAvailableOffset;
    }

    public void setNewestAvailableOffset(final String newestAvailableOffset) {
        this.newestAvailableOffset = newestAvailableOffset;
    }

    public Long getUnconsumedEvents() {
        return unconsumedEvents;
    }

    public void setUnconsumedEvents(final Long unconsumedEvents) {
        this.unconsumedEvents = unconsumedEvents;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }

        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        final EventTypePartitionView that = (EventTypePartitionView) o;

        if (!Objects.equals(partitionId, that.partitionId)) {
            return false;
        }

        if (!Objects.equals(oldestAvailableOffset, that.oldestAvailableOffset)) {
            return false;
        }

        if (!Objects.equals(unconsumedEvents, that.unconsumedEvents)) {
            return false;
        }

        return Objects.equals(newestAvailableOffset, that.newestAvailableOffset);

    }

    @Override
    public int hashCode() {
        return Objects.hash(partitionId, oldestAvailableOffset, newestAvailableOffset, unconsumedEvents);
    }

    @Override
    public String toString() {
        return "EventTypePartitionView{" +
                "partitionId='" + partitionId + '\'' +
                ", oldestAvailableOffset='" + oldestAvailableOffset + '\'' +
                ", newestAvailableOffset='" + newestAvailableOffset + '\'' +
                ", unconsumedEvents='" + unconsumedEvents + '\'' +
                '}';
    }
}
