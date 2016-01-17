package de.zalando.aruha.nakadi.repository;

import java.util.List;
import java.util.Map;

import de.zalando.aruha.nakadi.NakadiException;
import de.zalando.aruha.nakadi.domain.Topic;
import de.zalando.aruha.nakadi.domain.TopicPartition;

/**
 * Manages access to topic information.
 *
 * @author  john
 */
public interface TopicRepository {

  List<Topic> listTopics() throws NakadiException;

  void postEvent(String topicId, String partitionId, String payload) throws NakadiException;

  List<TopicPartition> listPartitions(String topicId) throws NakadiException;

  boolean validateOffset(String offsetToCheck, String newestOffset, String oldestOffset);

  void readEvent(String topicId, String partitionId);

  EventConsumer createEventConsumer(String topic, Map<String, String> cursors);
}
