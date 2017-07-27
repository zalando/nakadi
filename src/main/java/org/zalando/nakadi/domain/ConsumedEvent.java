package org.zalando.nakadi.domain;

import javax.annotation.concurrent.Immutable;
import java.util.Objects;

@Immutable
public class ConsumedEvent {

    private final byte[] event;
    private final NakadiCursor position;

    public ConsumedEvent(final byte[] event, final NakadiCursor position) {
        this.event = event;
        this.position = position;
    }

    public byte[] getEvent() {
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
