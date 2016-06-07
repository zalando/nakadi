package de.zalando.aruha.nakadi.repository;

import de.zalando.aruha.nakadi.domain.BatchItem;
import de.zalando.aruha.nakadi.domain.Cursor;
import de.zalando.aruha.nakadi.domain.EventTypeStatistics;
import de.zalando.aruha.nakadi.domain.Topic;
import de.zalando.aruha.nakadi.domain.TopicPartition;
import de.zalando.aruha.nakadi.exceptions.DuplicatedEventTypeNameException;
import de.zalando.aruha.nakadi.exceptions.EventPublishingException;
import de.zalando.aruha.nakadi.exceptions.InvalidCursorException;
import de.zalando.aruha.nakadi.exceptions.NakadiException;
import de.zalando.aruha.nakadi.exceptions.ServiceUnavailableException;
import de.zalando.aruha.nakadi.exceptions.TopicCreationException;
import de.zalando.aruha.nakadi.exceptions.TopicDeletionException;

import java.util.List;
import java.util.Map;

/**
 * Manages access to topic information.
 *
 * @author john
 */
public interface TopicRepository {

    List<Topic> listTopics() throws NakadiException;

    void createTopic(String topic, EventTypeStatistics defaultStatistics) throws TopicCreationException, DuplicatedEventTypeNameException;

    void deleteTopic(String topic) throws TopicDeletionException;

    boolean topicExists(String topic) throws NakadiException;

    boolean partitionExists(String topic, String partition) throws NakadiException;

    void syncPostBatch(String topicId, List<BatchItem> batch) throws EventPublishingException;

    List<TopicPartition> listPartitions(String topicId) throws NakadiException;

    Map<String, Long> materializePositions(final String topicId, String position) throws ServiceUnavailableException;

    List<String> listPartitionNames(final String topicId);

    TopicPartition getPartition(String topicId, String partition) throws NakadiException;

    EventConsumer createEventConsumer(String topic, List<Cursor> cursors) throws NakadiException, InvalidCursorException;


}
