package org.zalando.nakadi.service.timeline;

import org.zalando.nakadi.domain.EventType;
import org.zalando.nakadi.domain.Storage;
import org.zalando.nakadi.domain.Timeline;
import org.zalando.nakadi.domain.VersionedCursor;

import java.util.List;

public interface StorageWorker {

    Timeline.EventTypeConfiguration createEventTypeConfiguration(final EventType et, final boolean initial);

    List<VersionedCursor> getLatestPosition(Timeline activeTimeline);

    static StorageWorker build(final Storage storage) {
        switch (storage.getType()) {
            case KAFKA:
                return new KafkaStorageWorker((Storage.KafkaStorage) storage);
            default:
                throw new IllegalArgumentException("Storage type is not supported");
        }
    }

}
