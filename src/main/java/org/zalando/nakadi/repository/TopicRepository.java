package org.zalando.nakadi.repository;

import org.zalando.nakadi.domain.*;
import org.zalando.nakadi.exceptions.*;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Manages access to topic information.
 *
 * @author john
 */
public interface TopicRepository {

    List<Topic> listTopics() throws NakadiException;

    void deleteTopic(String topic) throws TopicDeletionException;

    boolean topicExists(Timeline.EventTypeConfiguration topic) throws NakadiException;

    boolean partitionExists(Timeline.EventTypeConfiguration topic, String partition) throws NakadiException;

    void syncPostBatch(Timeline timeline, List<BatchItem> batch) throws EventPublishingException;

    List<TopicPartition> listPartitions(Timeline.EventTypeConfiguration topic) throws NakadiException;

    List<TopicPartition> listPartitions(Set<String> topics) throws ServiceUnavailableException;

    Map<String, Long> materializePositions(String topicId, SubscriptionBase.InitialPosition position)
            throws ServiceUnavailableException;

    List<String> listPartitionNames(final Timeline.EventTypeConfiguration configuration);

    TopicPartition getPartition(String topicId, String partition) throws NakadiException;

    EventConsumer createEventConsumer(Timeline.EventTypeConfiguration cfg, List<Cursor> cursors) throws NakadiException,
            InvalidCursorException;

    void validateCommitCursors(Timeline.EventTypeConfiguration cfg, List<? extends Cursor> cursors) throws InvalidCursorException;
}
