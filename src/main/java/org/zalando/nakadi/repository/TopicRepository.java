package org.zalando.nakadi.repository;

import com.codahale.metrics.Meter;
import org.zalando.nakadi.domain.BatchItem;
import org.zalando.nakadi.domain.EventType;
import org.zalando.nakadi.domain.NakadiCursor;
import org.zalando.nakadi.domain.PartitionStatistics;
import org.zalando.nakadi.domain.Subscription;
import org.zalando.nakadi.exceptions.EventPublishingException;
import org.zalando.nakadi.exceptions.InvalidCursorException;
import org.zalando.nakadi.exceptions.NakadiException;
import org.zalando.nakadi.exceptions.ServiceUnavailableException;
import org.zalando.nakadi.exceptions.TopicCreationException;
import org.zalando.nakadi.exceptions.TopicDeletionException;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public interface TopicRepository {

    String createTopic(int partitionCount, Long retentionTimeMs) throws TopicCreationException;

    void deleteTopic(String topic) throws TopicDeletionException;

    boolean topicExists(String topic) throws NakadiException;

    void syncPostBatch(String topicId, List<BatchItem> batch, final Meter meter) throws EventPublishingException;

    Optional<PartitionStatistics> loadPartitionStatistics(String topic, String partition)
            throws ServiceUnavailableException;

    List<PartitionStatistics> loadTopicStatistics(Collection<String> topics) throws ServiceUnavailableException;

    Map<String, Long> materializePositions(EventType eventType, Subscription subscription)
            throws ServiceUnavailableException;

    List<String> listPartitionNames(final String topicId);

    EventConsumer createEventConsumer(String clientId, List<NakadiCursor> positions) throws NakadiException,
            InvalidCursorException;

    int compareOffsets(NakadiCursor first, NakadiCursor second) throws InvalidCursorException;

    void validateReadCursors(final List<NakadiCursor> cursors) throws InvalidCursorException,
            ServiceUnavailableException;

    void validateCommitCursor(NakadiCursor cursor) throws InvalidCursorException;
}
