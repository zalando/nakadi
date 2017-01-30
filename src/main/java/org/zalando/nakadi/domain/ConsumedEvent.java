package org.zalando.nakadi.domain;

import java.util.Objects;
import javax.annotation.concurrent.Immutable;

@Immutable
public class ConsumedEvent {

    private final String event;
    private final NakadiCursor position;

    public ConsumedEvent(final String event, final NakadiCursor position) {
        this.event = event;
        this.position = position;
    }

    public String getEvent() {
        return event;
    }

    public NakadiCursor getPosition() {
        return position;
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
