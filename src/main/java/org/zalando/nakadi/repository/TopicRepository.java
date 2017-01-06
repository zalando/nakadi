package org.zalando.nakadi.repository;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import org.zalando.nakadi.domain.BatchItem;
import org.zalando.nakadi.domain.SubscriptionBase;
import org.zalando.nakadi.domain.TopicPosition;
import org.zalando.nakadi.domain.PartitionStatistics;
import org.zalando.nakadi.exceptions.DuplicatedEventTypeNameException;
import org.zalando.nakadi.exceptions.EventPublishingException;
import org.zalando.nakadi.exceptions.InternalNakadiException;
import org.zalando.nakadi.exceptions.InvalidCursorException;
import org.zalando.nakadi.exceptions.NakadiException;
import org.zalando.nakadi.exceptions.ServiceUnavailableException;
import org.zalando.nakadi.exceptions.TopicCreationException;
import org.zalando.nakadi.exceptions.TopicDeletionException;

public interface TopicRepository {

    String createTopic(int partitionCount, Long retentionTimeMs)
            throws TopicCreationException, DuplicatedEventTypeNameException;

    void deleteTopic(String topic) throws TopicDeletionException;

    boolean topicExists(String topic) throws NakadiException;

    void syncPostBatch(String topicId, List<BatchItem> batch) throws EventPublishingException;

    List<PartitionStatistics> loadTopicStatistics(Collection<String> topics) throws ServiceUnavailableException;

    Map<String, Long> materializePositions(String topicId, SubscriptionBase.InitialPosition position)
            throws ServiceUnavailableException;

    List<String> listPartitionNames(final String topicId);

    EventConsumer createEventConsumer(String clientId, List<TopicPosition> positions) throws NakadiException,
            InvalidCursorException;

    int compareOffsets(final TopicPosition first, final TopicPosition second) throws InternalNakadiException;

    void validateCommitCursor(TopicPosition cursor) throws InvalidCursorException;
}
