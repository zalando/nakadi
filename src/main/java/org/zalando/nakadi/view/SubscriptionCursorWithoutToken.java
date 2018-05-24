package org.zalando.nakadi.view;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.zalando.nakadi.domain.EventTypePartition;

import javax.annotation.concurrent.Immutable;
import javax.validation.constraints.NotNull;

@Immutable
public class SubscriptionCursorWithoutToken extends Cursor {

    @NotNull
    private final String eventType;

    public SubscriptionCursorWithoutToken(@JsonProperty("event_type") final String eventType,
                                          @JsonProperty("partition") final String partition,
                                          @JsonProperty("offset") final String offset) {
        super(partition, offset);
        this.eventType = eventType;
    }

    public String getEventType() {
        return eventType;
    }

    @JsonIgnore
    public EventTypePartition getEventTypePartition() {
        return new EventTypePartition(eventType, getPartition());
    }

    public SubscriptionCursor withToken(final String token) {
        return new SubscriptionCursor(getPartition(), getOffset(), getEventType(), token);
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        if (!super.equals(o)) {
            return false;
        }
        final SubscriptionCursorWithoutToken that = (SubscriptionCursorWithoutToken) o;
        return eventType.equals(that.eventType);
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        // eventType is checked for null here only because of validation implementation that
        // calls hashCode before checking fields for not-null
        result = 31 * result + (eventType != null ? eventType.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "SubscriptionCursor{" +
                "partition='" + getPartition() + '\'' +
                ", offset='" + getOffset() + '\'' +
                ", eventType='" + eventType + '\'' +
                '}';
    }
}
