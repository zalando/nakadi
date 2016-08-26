package org.zalando.nakadi.repository;

import org.zalando.nakadi.domain.BatchItem;
import org.zalando.nakadi.domain.Cursor;
import org.zalando.nakadi.domain.EventType;
import org.zalando.nakadi.domain.SubscriptionBase;
import org.zalando.nakadi.domain.Topic;
import org.zalando.nakadi.domain.TopicPartition;
import org.zalando.nakadi.exceptions.DuplicatedEventTypeNameException;
import org.zalando.nakadi.exceptions.EventPublishingException;
import org.zalando.nakadi.exceptions.InternalNakadiException;
import org.zalando.nakadi.exceptions.InvalidCursorException;
import org.zalando.nakadi.exceptions.NakadiException;
import org.zalando.nakadi.exceptions.ServiceUnavailableException;
import org.zalando.nakadi.exceptions.TopicCreationException;
import org.zalando.nakadi.exceptions.TopicDeletionException;

import java.util.List;
import java.util.Map;

/**
 * Manages access to topic information.
 *
 * @author john
 */
public interface TopicRepository {

    List<Topic> listTopics() throws NakadiException;

    void createTopic(EventType eventType) throws TopicCreationException, DuplicatedEventTypeNameException;

    void deleteTopic(String topic) throws TopicDeletionException;

    boolean topicExists(String topic) throws NakadiException;

    boolean partitionExists(String topic, String partition) throws NakadiException;

    void syncPostBatch(String topicId, List<BatchItem> batch) throws EventPublishingException;

    List<TopicPartition> listPartitions(String topicId) throws NakadiException;

    Map<String, Long> materializePositions(String topicId, SubscriptionBase.InitialPosition position)
            throws ServiceUnavailableException;

    List<String> listPartitionNames(final String topicId);

    TopicPartition getPartition(String topicId, String partition) throws NakadiException;

    EventConsumer createEventConsumer(String topic, List<Cursor> cursors) throws NakadiException,
            InvalidCursorException;

    int compareOffsets(String firstOffset, String secondOffset) throws InternalNakadiException;

    void validateCommitCursors(String topic, List<Cursor> cursors) throws InvalidCursorException;
}
