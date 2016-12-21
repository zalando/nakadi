package org.zalando.nakadi.service.timeline;

import org.zalando.nakadi.domain.EventType;
import org.zalando.nakadi.domain.Storage;
import org.zalando.nakadi.domain.Timeline;
import org.zalando.nakadi.domain.VersionedCursor;

import java.util.List;

public class KafkaStorageWorker implements StorageWorker {
    private final Storage.KafkaStorage storage;

    KafkaStorageWorker(final Storage.KafkaStorage storage) {
        this.storage = storage;
    }

    @Override
    public Timeline.KafkaEventTypeConfiguration createEventTypeConfiguration(
            final EventType et, final boolean initial) {
        final Timeline.KafkaEventTypeConfiguration result = new Timeline.KafkaEventTypeConfiguration();
        if (initial) {
            result.setTopicName(et.getTopic());
        } else {
            // TODO: create topic.
            throw new UnsupportedOperationException("Not supported yet");
        }
        return result;
    }

    @Override
    public List<VersionedCursor> getLatestPosition(final Timeline activeTimeline) {
        throw new UnsupportedOperationException("Not supported yet");
    }
}
