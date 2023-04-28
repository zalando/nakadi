package org.zalando.nakadi.repository;

import org.zalando.nakadi.domain.ConsumedEvent;
import org.zalando.nakadi.domain.EventTypePartition;
import org.zalando.nakadi.domain.NakadiCursor;
import org.zalando.nakadi.domain.TopicPartition;
import org.zalando.nakadi.exceptions.runtime.InvalidCursorException;

import java.io.Closeable;
import java.util.Collection;
import java.util.List;
import java.util.Set;

public interface EventConsumer extends Closeable {

    List<ConsumedEvent> readEvents();

    void reassign(Collection<NakadiCursor> cursors) throws InvalidCursorException;

    interface LowLevelConsumer extends EventConsumer {
        Set<TopicPartition> getAssignment();
    }

    interface HighLevelConsumer extends EventConsumer {
        Set<EventTypePartition> getAssignment();
    }
}
