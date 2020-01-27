package org.zalando.nakadi.domain;

import javax.annotation.Nullable;
import javax.annotation.concurrent.Immutable;
import java.util.Objects;

@Immutable
public class ConsumedEvent {

    private final byte[] event;
    private final NakadiCursor position;
    private final long timestamp;
    private final EventAuthField eventAuthField;

    public ConsumedEvent(final byte[] event, final NakadiCursor position, final long timestamp,
                         @Nullable final EventAuthField eventAuthField) {
        this.event = event;
        this.position = position;
        this.timestamp = timestamp;
        this.eventAuthField = eventAuthField;
    }

    public byte[] getEvent() {
        return event;
    }

    public NakadiCursor getPosition() {
        return position;
    }

    public long getTimestamp() {
        return timestamp;
    }

    @Nullable
    public EventAuthField getEventAuthField() {
        return eventAuthField;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof ConsumedEvent)) {
            return false;
        }

        final ConsumedEvent that = (ConsumedEvent) o;
        return Objects.equals(this.event, that.event)
                && Objects.equals(this.position, that.position);
    }

    @Override
    public int hashCode() {
        return position.hashCode();
    }
}
