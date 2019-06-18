package org.zalando.nakadi.service;

import org.zalando.nakadi.domain.storage.Storage;
import org.zalando.nakadi.domain.Timeline;
import org.zalando.nakadi.exceptions.runtime.InvalidCursorOperation;
import org.zalando.nakadi.repository.kafka.KafkaCursor;

import java.util.EnumMap;

public class StaticStorageWorkerFactory {

    public interface StaticStorageWorker {
        long totalEventsInPartition(Timeline timeline, String partitionString);

        String getBeforeFirstOffset();

    }

    public static StaticStorageWorker get(final Storage storage) {
        return WORKER_MAP.get(storage.getType());
    }

    public static StaticStorageWorker get(final Timeline timeline) {
        return get(timeline.getStorage());
    }

    private static class StaticKafkaStorageWorker implements StaticStorageWorker {

        @Override
        public long totalEventsInPartition(final Timeline timeline, final String partitionString) {
            final Timeline.StoragePosition positions = timeline.getLatestPosition();

            try {
                return 1 + ((Timeline.KafkaStoragePosition) positions).getLastOffsetForPartition(
                        KafkaCursor.toKafkaPartition(partitionString));
            } catch (final IllegalArgumentException ex) {
                throw new InvalidCursorOperation(InvalidCursorOperation.Reason.PARTITION_NOT_FOUND);
            }

        }

        @Override
        public String getBeforeFirstOffset() {
            return "-1"; // Yes, it is always like that for kafka.
        }

    }

    private static final EnumMap<Storage.Type, StaticStorageWorker> WORKER_MAP =
            new EnumMap<>(Storage.Type.class);

    static {
        WORKER_MAP.put(Storage.Type.KAFKA, new StaticKafkaStorageWorker());
    }


}
