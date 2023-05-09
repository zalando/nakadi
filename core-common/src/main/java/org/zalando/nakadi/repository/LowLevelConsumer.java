package org.zalando.nakadi.repository;

import org.zalando.nakadi.domain.EventOwnerHeader;
import org.zalando.nakadi.domain.NakadiCursor;
import org.zalando.nakadi.domain.TopicPartition;
import org.zalando.nakadi.exceptions.runtime.InvalidCursorException;

import java.io.Closeable;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.Set;

public interface LowLevelConsumer extends Closeable {

    Set<TopicPartition> getAssignment();

    List<Event> readEvents();

    void reassign(Collection<NakadiCursor> cursors) throws InvalidCursorException;

    class Event {
        private final byte[] data;
        private final String topic;
        private final int partition;
        private final long offset;
        private final long timestamp;
        private final EventOwnerHeader eventOwnerHeader;
        private final String subscriptionId;

        public Event(final byte[] data,
                     final String topic,
                     final int partition,
                     final long offset,
                     final long timestamp,
                     final EventOwnerHeader eventOwnerHeader,
                     final String subscriptionId) {
            this.data = data;
            this.topic = topic;
            this.partition = partition;
            this.offset = offset;
            this.timestamp = timestamp;
            this.eventOwnerHeader = eventOwnerHeader;
            this.subscriptionId = subscriptionId;
        }

        public byte[] getData() {
            return data;
        }

        public String getTopic() {
            return topic;
        }

        public int getPartition() {
            return partition;
        }

        public long getOffset() {
            return offset;
        }

        public long getTimestamp() {
            return timestamp;
        }

        public EventOwnerHeader getEventOwnerHeader() {
            return eventOwnerHeader;
        }

        public String getSubscriptionId() {
            return subscriptionId;
        }

        @Override
        public boolean equals(final Object o) {
            if (this == o) {
                return true;
            }

            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            final Event event = (Event) o;
            return timestamp == event.timestamp &&
                    Arrays.equals(data, event.data) &&
                    Objects.equals(topic, event.topic) &&
                    Objects.equals(partition, event.partition) &&
                    Objects.equals(offset, event.offset) &&
                    Objects.equals(eventOwnerHeader, event.eventOwnerHeader) &&
                    Objects.equals(subscriptionId, event.subscriptionId);
        }

        @Override
        public int hashCode() {
            int result = Objects.hash(topic, partition, offset, timestamp, eventOwnerHeader, subscriptionId);
            result = 31 * result + Arrays.hashCode(data);
            return result;
        }
    }
}
