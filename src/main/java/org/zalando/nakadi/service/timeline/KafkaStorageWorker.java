package org.zalando.nakadi.service.timeline;

import org.zalando.nakadi.config.NakadiSettings;
import org.zalando.nakadi.domain.*;
import org.zalando.nakadi.domain.VersionedCursor.VersionedCursorV1;
import org.zalando.nakadi.exceptions.NakadiException;
import org.zalando.nakadi.repository.kafka.KafkaPartitionsCalculator;
import org.zalando.nakadi.repository.kafka.KafkaTopicRepository;

import javax.annotation.Nullable;
import java.util.List;
import java.util.stream.Collectors;

public class KafkaStorageWorker implements StorageWorker {
    private final Storage.KafkaStorage storage;
    private final KafkaPartitionsCalculator partitionsCalculator;
    private final NakadiSettings nakadiSettings;

    KafkaStorageWorker(
            final Storage.KafkaStorage storage,
            final KafkaPartitionsCalculator partitionsCalculator,
            final NakadiSettings nakadiSettings) {
        this.storage = storage;
        this.partitionsCalculator = partitionsCalculator;
        this.nakadiSettings = nakadiSettings;
    }

    @Override
    public Storage getStorage() {
        return storage;
    }

    @Override
    public Timeline.KafkaEventTypeConfiguration createEventTypeConfiguration(
            final EventType et, @Nullable final Integer partitionCount, final boolean initial) throws NakadiException {
        final Timeline.KafkaEventTypeConfiguration result = new Timeline.KafkaEventTypeConfiguration();
        if (initial) {
            result.setTopicName(et.getTopic());
        } else {
            result.setTopicName(
                    getTopicRepository().createTopic(
                            null == partitionCount ? calculateKafkaPartitionCount(et.getDefaultStatistic()) : partitionCount,
                            et));
        }
        return result;
    }

    @Override
    public List<VersionedCursor> getLatestPosition(final Timeline activeTimeline) throws NakadiException {
        final Timeline.KafkaEventTypeConfiguration cfg =
                (Timeline.KafkaEventTypeConfiguration) activeTimeline.getStorageConfiguration();

        final List<TopicPartition> result = getTopicRepository().listPartitions(cfg);
        return result.stream().map(tp ->
                new VersionedCursorV1(tp.getPartitionId(), activeTimeline.getId(), tp.getNewestAvailableOffset()))
                .collect(Collectors.toList());
    }

    @Override
    public KafkaTopicRepository getTopicRepository() {
        return null;
    }

    @Override
    public Timeline createFakeTimeline(final EventType eventType) {
        final Timeline timeline = new Timeline();
        final Timeline.KafkaEventTypeConfiguration cfg = new Timeline.KafkaEventTypeConfiguration();
        cfg.setTopicName(eventType.getTopic());
        timeline.setOrder(0);
        timeline.setStorageConfiguration(cfg);
        timeline.setEventType(eventType.getName());
        timeline.setStorage(storage);
        return timeline;
    }

    @Override
    public int compare(final VersionedCursor cursor1, final VersionedCursor cursor2) {
        return Long.compare(extractOffset(cursor1), extractOffset(cursor2));
    }

    private long extractOffset(final VersionedCursor cursor) {
        if (cursor instanceof VersionedCursorV1) {
            return Long.parseLong(((VersionedCursorV1) cursor).getOffset());
        } else if (cursor instanceof VersionedCursor.VersionedCursorV0) {
            return Long.parseLong(((VersionedCursor.VersionedCursorV0) cursor).getOffset());
        } else {
            throw new IllegalArgumentException("Cursor type not supported: " + cursor.getClass());
        }
    }

    private Integer calculateKafkaPartitionCount(final EventTypeStatistics stat) {
        if (null == stat) {
            return nakadiSettings.getDefaultTopicPartitionCount();
        }
        final int maxPartitionsDueParallelism = Math.max(stat.getReadParallelism(), stat.getWriteParallelism());
        if (maxPartitionsDueParallelism >= nakadiSettings.getMaxTopicPartitionCount()) {
            return nakadiSettings.getMaxTopicPartitionCount();
        }
        return Math.min(nakadiSettings.getMaxTopicPartitionCount(), Math.max(
                maxPartitionsDueParallelism,
                calculatePartitionsAccordingLoad(stat.getMessagesPerMinute(), stat.getMessageSize())));
    }

    private int calculatePartitionsAccordingLoad(final int messagesPerMinute, final int avgEventSizeBytes) {
        final float throughoutputMbPerSec = ((float) messagesPerMinute * (float) avgEventSizeBytes)
                / (1024.f * 1024.f * 60.f);
        return partitionsCalculator.getBestPartitionsCount(avgEventSizeBytes, throughoutputMbPerSec);
    }
}
