package de.zalando.aruha.nakadi.repository;

import java.util.List;

import de.zalando.aruha.nakadi.NakadiException;
import de.zalando.aruha.nakadi.domain.Cursor;
import de.zalando.aruha.nakadi.domain.Topic;
import de.zalando.aruha.nakadi.domain.TopicPartitionOffsets;

/**
 * Manages access to topic information.
 *
 * @author  john
 */
public interface TopicRepository {

    List<Topic> listTopics() throws NakadiException;

    void postEvent(String topicId, String partitionId, String payload) throws NakadiException;

    List<String> listPartitions(String topic);

    List<TopicPartitionOffsets> listPartitionsOffsets(String topicId);

    boolean validateOffset(String offsetToCheck, String newestOffset, String oldestOffset);

    EventConsumer createEventConsumer(List<Cursor> cursors);
}
