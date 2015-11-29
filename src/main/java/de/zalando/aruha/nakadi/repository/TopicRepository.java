package de.zalando.aruha.nakadi.repository;

import java.util.List;

import de.zalando.aruha.nakadi.domain.Topic;
import de.zalando.aruha.nakadi.domain.TopicPartition;

/**
 * Manages access to topic information.
 *
 * @author  john
 */
public interface TopicRepository {

    List<Topic> listTopics();

    void postMessage(String topicId, String partitionId, String payload);

	List<TopicPartition> listPartitions(String topicId);

}
