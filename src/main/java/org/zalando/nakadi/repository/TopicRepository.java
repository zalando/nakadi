package org.zalando.nakadi.repository;

import java.util.Collection;
import java.util.List;
import java.util.Optional;
import org.zalando.nakadi.domain.BatchItem;
import org.zalando.nakadi.domain.NakadiCursor;
import org.zalando.nakadi.domain.PartitionStatistics;
import org.zalando.nakadi.domain.Timeline;
import org.zalando.nakadi.exceptions.EventPublishingException;
import org.zalando.nakadi.exceptions.InvalidCursorException;
import org.zalando.nakadi.exceptions.NakadiException;
import org.zalando.nakadi.exceptions.ServiceUnavailableException;
import org.zalando.nakadi.exceptions.TopicCreationException;
import org.zalando.nakadi.exceptions.TopicDeletionException;

public interface TopicRepository {

    String createTopic(int partitionCount, Long retentionTimeMs) throws TopicCreationException;

    void deleteTopic(String topic) throws TopicDeletionException;

    boolean topicExists(String topic) throws NakadiException;

    void syncPostBatch(String topicId, List<BatchItem> batch) throws EventPublishingException;

    Optional<PartitionStatistics> loadPartitionStatistics(Timeline timeline, String partition)
            throws ServiceUnavailableException;

    List<PartitionStatistics> loadTopicStatistics(Collection<Timeline> topics) throws ServiceUnavailableException;

    List<String> listPartitionNames(String topicId);

    EventConsumer createEventConsumer(String clientId, List<NakadiCursor> positions) throws NakadiException,
            InvalidCursorException;

    int compareOffsets(NakadiCursor first, NakadiCursor second) throws InvalidCursorException;

    void validateReadCursors(List<NakadiCursor> cursors) throws InvalidCursorException,
            ServiceUnavailableException;

    void validateCommitCursor(NakadiCursor cursor) throws InvalidCursorException;
}
