package org.zalando.nakadi.repository;

import org.zalando.nakadi.domain.ConsumedEvent;
import org.zalando.nakadi.domain.EventTypePartition;
import org.zalando.nakadi.domain.NakadiCursor;
import org.zalando.nakadi.domain.PartitionStatistics;
import org.zalando.nakadi.domain.TopicPartition;
import org.zalando.nakadi.exceptions.runtime.InvalidCursorException;

import java.io.Closeable;
import java.util.Collection;
import java.util.List;
import java.util.Set;

public interface EventConsumer extends Closeable {

    List<ConsumedEvent> readEvents();

    interface LowLevelConsumer extends EventConsumer {
        Set<TopicPartition> getAssignment();

        default void reassign(Collection<NakadiCursor> cursors) throws InvalidCursorException {
        }
    }

    interface ReassignableEventConsumer extends EventConsumer {
        Set<EventTypePartition> getEventTypeAssignment();

        void reassign(Collection<NakadiCursor> newValues) throws InvalidCursorException;
    }
}
