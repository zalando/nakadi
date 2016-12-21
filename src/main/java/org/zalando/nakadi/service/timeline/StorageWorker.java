package org.zalando.nakadi.service.timeline;

import org.zalando.nakadi.domain.EventType;
import org.zalando.nakadi.domain.Storage;
import org.zalando.nakadi.domain.Timeline;
import org.zalando.nakadi.domain.VersionedCursor;
import org.zalando.nakadi.exceptions.NakadiException;
import org.zalando.nakadi.repository.TopicRepository;

import javax.annotation.Nullable;
import java.util.List;

public interface StorageWorker {

    Storage getStorage();

    Timeline.EventTypeConfiguration createEventTypeConfiguration(
            final EventType et,
            @Nullable final Integer partitionCount,
            boolean importFromOld) throws NakadiException;

    List<VersionedCursor> getLatestPosition(Timeline activeTimeline) throws NakadiException;

    TopicRepository getTopicRepository();

    Timeline createFakeTimeline(EventType eventType);

    int compare(VersionedCursor cursor, VersionedCursor currentCursor);
}
