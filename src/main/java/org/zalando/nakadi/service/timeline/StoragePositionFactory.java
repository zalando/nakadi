package org.zalando.nakadi.service.timeline;

import org.zalando.nakadi.domain.NakadiCursor;
import org.zalando.nakadi.domain.PartitionStatistics;
import org.zalando.nakadi.domain.Timeline;
import org.zalando.nakadi.exceptions.NakadiRuntimeException;
import org.zalando.nakadi.exceptions.ServiceUnavailableException;
import org.zalando.nakadi.repository.TopicRepository;

import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

public class StoragePositionFactory {

    public static Timeline.StoragePosition createStoragePosition(final Timeline activeTimeline,
                                                                 final TopicRepository topicRepo)
            throws NakadiRuntimeException {
        try {
            final List<NakadiCursor> offsets =
                    topicRepo.loadTopicStatistics(Collections.singleton(activeTimeline.getTopic())).stream()
                            .map(PartitionStatistics::getLast)
                            .collect(Collectors.toList());

            switch (activeTimeline.getStorage().getType()) {
                case KAFKA:
                    return kafkaStoragePosition(offsets);
                default:
                    throw new IllegalStateException();
            }
        } catch (final ServiceUnavailableException sue) {
            throw new NakadiRuntimeException(sue);
        }
    }

    private static Timeline.StoragePosition kafkaStoragePosition(final List<NakadiCursor> offsets) {
        final Timeline.KafkaStoragePosition kafkaStoragePosition = new Timeline.KafkaStoragePosition();
        kafkaStoragePosition.setOffsets(offsets.stream()
                .sorted(Comparator.comparing(p -> Integer.valueOf(p.getPartition())))
                .map(nakadiCursor -> Long.valueOf(nakadiCursor.getOffset()))
                .collect(Collectors.toList()));
        return kafkaStoragePosition;
    }
}
