package de.zalando.aruha.nakadi.repository;

import de.zalando.aruha.nakadi.domain.Cursor;
import de.zalando.aruha.nakadi.domain.Topic;
import de.zalando.aruha.nakadi.domain.TopicPartition;
import de.zalando.aruha.nakadi.exceptions.DuplicatedEventTypeNameException;
import de.zalando.aruha.nakadi.exceptions.NakadiException;
import de.zalando.aruha.nakadi.exceptions.TopicCreationException;
import de.zalando.aruha.nakadi.exceptions.TopicDeletionException;

import java.util.List;
import java.util.Map;

/**
 * Manages access to topic information.
 *
 * @author  john
 */
public interface TopicRepository {

    List<Topic> listTopics() throws NakadiException;

    void createTopic(String topic) throws TopicCreationException, DuplicatedEventTypeNameException;

    void createTopic(String topic, int partitionsNum, int replicaFactor, long retentionMs, long rotationMs)
            throws TopicCreationException, DuplicatedEventTypeNameException;

    void deleteTopic(String topic) throws TopicDeletionException;

    boolean topicExists(String topic) throws NakadiException;

    boolean partitionExists(String topic, String partition) throws NakadiException;

    boolean areCursorsValid(String topic, List<Cursor> cursors) throws NakadiException;

    void postEvent(String topicId, String partitionId, String payload) throws NakadiException;

    List<TopicPartition> listPartitions(String topicId) throws NakadiException;

    List<String> listPartitionNames(final String topicId) throws NakadiException;

    TopicPartition getPartition(String topicId, String partition) throws NakadiException;

    EventConsumer createEventConsumer(String topic, Map<String, String> cursors) throws NakadiException;
}
