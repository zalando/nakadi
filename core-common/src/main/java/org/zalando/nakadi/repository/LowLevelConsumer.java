package org.zalando.nakadi.repository;

import org.zalando.nakadi.domain.HeaderTag;
import org.zalando.nakadi.domain.EventOwnerHeader;
import org.zalando.nakadi.domain.NakadiCursor;
import org.zalando.nakadi.domain.TopicPartition;
import org.zalando.nakadi.exceptions.runtime.InvalidCursorException;

import java.io.Closeable;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
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
        private final Map<HeaderTag, String> consumerTags;

        public Event(final byte[] data,
                     final String topic,
                     final int partition,
                     final long offset,
                     final long timestamp,
                     final EventOwnerHeader eventOwnerHeader,
                     final Map<HeaderTag, String> consumerTags) {
            this.data = data;
            this.topic = topic;
            this.partition = partition;
            this.offset = offset;
            this.timestamp = timestamp;
            this.eventOwnerHeader = eventOwnerHeader;
            this.consumerTags = consumerTags;
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

        public Map<HeaderTag, String> getConsumerTags() {
            return consumerTags;
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
                    Objects.equals(consumerTags, event.consumerTags);
        }

        @Override
        public int hashCode() {
            int result = Objects.hash(topic, partition, offset, timestamp, eventOwnerHeader, consumerTags);
            result = 31 * result + Arrays.hashCode(data);
            return result;
        }
    }
}
