package de.zalando.aruha.nakadi.repository;

import java.util.List;
import java.util.Map;

import de.zalando.aruha.nakadi.NakadiException;
import de.zalando.aruha.nakadi.domain.Cursor;
import de.zalando.aruha.nakadi.domain.Topic;
import de.zalando.aruha.nakadi.domain.TopicPartition;

/**
 * Manages access to topic information.
 *
 * @author  john
 */
public interface TopicRepository {

    List<Topic> listTopics() throws NakadiException;

    void createTopic(String topic);

    void createTopic(String topic, int partitionsNum, int replicaFactor, long retentionMs, long rotationMs);

    boolean topicExists(String topic) throws NakadiException;

    boolean areCursorsCorrect(String topic, List<Cursor> cursors);

    void postEvent(String topicId, String partitionId, String payload) throws NakadiException;

    List<TopicPartition> listPartitions(String topicId);

    boolean validateOffset(String offsetToCheck, String newestOffset, String oldestOffset);

    EventConsumer createEventConsumer(String topic, Map<String, String> cursors);
}
