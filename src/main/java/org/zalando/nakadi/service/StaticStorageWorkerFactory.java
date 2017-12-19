package org.zalando.nakadi.service;

import org.zalando.nakadi.domain.Storage;
import org.zalando.nakadi.domain.Timeline;
import org.zalando.nakadi.exceptions.runtime.InvalidCursorOperation;
import org.zalando.nakadi.repository.kafka.KafkaCursor;

import java.util.EnumMap;

public class StaticStorageWorkerFactory {

    public interface StaticStorageWorker {
        long totalEventsInPartition(Timeline timeline, String partitionString);

        boolean isLastOffsetForPartition(Timeline timeline, String partitionString, String offset);

        boolean isInitialOffset(String offset);

        String getFirstOffsetInTimeline(String partition);
    }

    public static StaticStorageWorker get(final Storage storage) {
        return WORKER_MAP.get(storage.getType());
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
        public boolean isLastOffsetForPartition(
                final Timeline timeline, final String partitionString, final String offset) {
            if (null == timeline.getLatestPosition()) {
                return false;
            }
            final int partition = KafkaCursor.toKafkaPartition(partitionString);
            final long existingOffset = ((Timeline.KafkaStoragePosition) timeline.getLatestPosition())
                    .getLastOffsetForPartition(partition);
            final long checkedOffset = KafkaCursor.toKafkaOffset(offset);
            return existingOffset == checkedOffset;
        }

        @Override
        public boolean isInitialOffset(final String offset) {
            return Long.parseLong(offset) == -1; // Yes, it is always like that for kafka.
        }

        @Override
        public String getFirstOffsetInTimeline(final String partition) {
            return "-1"; // Yes, it is always like that for kafka.
        }
    }

    private static final EnumMap<Storage.Type, StaticStorageWorker> WORKER_MAP =
            new EnumMap<>(Storage.Type.class);

    static {
        WORKER_MAP.put(Storage.Type.KAFKA, new StaticKafkaStorageWorker());
    }


}
