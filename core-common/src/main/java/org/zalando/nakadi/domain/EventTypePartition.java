package org.zalando.nakadi.domain;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.zalando.nakadi.view.SubscriptionCursorWithoutToken;

import javax.annotation.concurrent.Immutable;
import javax.validation.constraints.NotNull;

@Immutable
public class EventTypePartition {

    @NotNull
    private final String eventType;

    @NotNull
    private final String partition;

    @JsonCreator
    public EventTypePartition(@JsonProperty("event_type") final String eventType,
                              @JsonProperty("partition") final String partition) {
        this.eventType = eventType;
        this.partition = partition;
    }

    public String getEventType() {
        return eventType;
    }

    public String getPartition() {
        return partition;
    }

    public boolean ownsCursor(final SubscriptionCursorWithoutToken cursor) {
        return eventType.equals(cursor.getEventType()) && partition.equals(cursor.getPartition());
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final EventTypePartition that = (EventTypePartition) o;
        return eventType.equals(that.eventType) && partition.equals(that.partition);
    }

    @Override
    public int hashCode() {
        int result = eventType.hashCode();
        result = 31 * result + partition.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return "EventTypePartition{" +
                "eventType='" + eventType + '\'' +
                ", partition='" + partition + '\'' +
                '}';
    }
}
