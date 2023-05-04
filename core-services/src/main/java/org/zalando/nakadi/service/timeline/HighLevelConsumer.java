package org.zalando.nakadi.service.timeline;

import org.zalando.nakadi.domain.ConsumedEvent;
import org.zalando.nakadi.domain.EventTypePartition;
import org.zalando.nakadi.domain.NakadiCursor;
import org.zalando.nakadi.exceptions.runtime.InvalidCursorException;

import java.io.Closeable;
import java.util.Collection;
import java.util.List;
import java.util.Set;

public interface HighLevelConsumer extends Closeable {

    Set<EventTypePartition> getAssignment();

    List<ConsumedEvent> readEvents();

    void reassign(Collection<NakadiCursor> cursors) throws InvalidCursorException;

}
