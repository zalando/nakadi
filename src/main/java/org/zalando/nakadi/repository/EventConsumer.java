package org.zalando.nakadi.repository;

import java.io.Closeable;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import org.zalando.nakadi.domain.ConsumedEvent;
import org.zalando.nakadi.domain.NakadiCursor;
import org.zalando.nakadi.domain.TopicPartition;
import org.zalando.nakadi.exceptions.InvalidCursorException;
import org.zalando.nakadi.exceptions.NakadiException;

public interface EventConsumer extends Closeable {

    Set<TopicPartition> getAssignment();

    List<ConsumedEvent> readEvents();

    interface ReassignableEventConsumer extends EventConsumer {
        void reassign(Collection<NakadiCursor> newValues) throws NakadiException, InvalidCursorException;
    }
}
