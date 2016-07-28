package org.zalando.nakadi.domain;

import javax.annotation.concurrent.Immutable;

@Immutable
public class ConsumedEvent {

    private final String event;
    private final String topic;
    private final String partition;
    private final String offset;

    public ConsumedEvent(final String event, final String topic, final String partition, final String offset) {
        this.event = event;
        this.topic = topic;
        this.partition = partition;
        this.offset = offset;
    }

    public String getEvent() {
        return event;
    }

    public String getTopic() {
        return topic;
    }

    public String getPartition() {
        return partition;
    }

    public String getOffset() {
        return offset;
    }

    @Override
    public boolean equals(final Object obj) {
        if (obj == null) {
            return false;
        }

        if (this == obj) {
            return true;
        }

        if (!(obj instanceof ConsumedEvent)) {
            return false;
        }

        final ConsumedEvent consumedEvent = (ConsumedEvent) obj;
        return this.event.equals(consumedEvent.getEvent()) && this.partition.equals(consumedEvent.getPartition())
                && this.offset.equals(consumedEvent.getOffset()) && this.topic.equals(consumedEvent.getTopic());
    }

    @Override
    public int hashCode() {
        int result = event.hashCode();
        result = 31 * result + topic.hashCode();
        result = 31 * result + partition.hashCode();
        result = 31 * result + offset.hashCode();
        return result;
    }
}
