package de.zalando.aruha.nakadi.repository;

import java.io.IOException;
import java.util.List;

import org.apache.zookeeper.KeeperException;

import de.zalando.aruha.nakadi.NakadiException;
import de.zalando.aruha.nakadi.domain.Topic;
import de.zalando.aruha.nakadi.domain.TopicPartition;

/**
 * Manages access to topic information.
 *
 * @author john
 */
public interface TopicRepository {

	List<Topic> listTopics() throws NakadiException;

	void postEvent(String topicId, String partitionId, String payload);

	List<TopicPartition> listPartitions(String topicId) throws NakadiException;

}
