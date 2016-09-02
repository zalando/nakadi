package org.zalando.nakadi.domain;

import com.fasterxml.jackson.annotation.JsonProperty;

import javax.annotation.concurrent.Immutable;

@Immutable
public class SubscriptionCursor extends Cursor {

    private final String eventType;

    private final String cursorToken;

    public SubscriptionCursor(@JsonProperty("partition") final String partition,
                              @JsonProperty("offset") final String offset,
                              @JsonProperty("event_type") final String eventType,
                              @JsonProperty("cursor_token") final String cursorToken) {
        super(partition, offset);
        this.eventType = eventType;
        this.cursorToken = cursorToken;
    }

    public String getEventType() {
        return eventType;
    }

    public String getCursorToken() {
        return cursorToken;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (!super.equals(o)) return false;
        final SubscriptionCursor that = (SubscriptionCursor) o;
        return eventType.equals(that.eventType) && cursorToken.equals(that.cursorToken);
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + eventType.hashCode();
        result = 31 * result + cursorToken.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return "SubscriptionCursor{" +
                "partition='" + getPartition() + '\'' +
                ", offset='" + getOffset() + '\'' +
                ", eventType='" + eventType + '\'' +
                ", cursorToken='" + cursorToken + '\'' +
                '}';
    }
}
