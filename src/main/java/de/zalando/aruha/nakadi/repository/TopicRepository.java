package de.zalando.aruha.nakadi.repository;

import java.util.List;
import java.util.Map;

import de.zalando.aruha.nakadi.NakadiException;
import de.zalando.aruha.nakadi.domain.Topic;
import de.zalando.aruha.nakadi.domain.TopicPartitionOffsets;
import org.apache.kafka.common.PartitionInfo;

/**
 * Manages access to topic information.
 *
 * @author  john
 */
public interface TopicRepository {

    List<Topic> listTopics() throws NakadiException;

    void postEvent(String topicId, String partitionId, String payload) throws NakadiException;

    List<String> listPartitions(String topic);

    List<TopicPartitionOffsets> listPartitionsOffsets(String topicId) throws NakadiException;

    boolean validateOffset(String offsetToCheck, String newestOffset, String oldestOffset);

    EventConsumer createEventConsumer();
}
