package de.zalando.aruha.nakadi.repository;

import de.zalando.aruha.nakadi.domain.Cursor;
import de.zalando.aruha.nakadi.domain.Topic;
import de.zalando.aruha.nakadi.domain.TopicPartition;
import de.zalando.aruha.nakadi.exceptions.InternalNakadiException;
import de.zalando.aruha.nakadi.exceptions.NakadiException;
import de.zalando.aruha.nakadi.exceptions.TopicDeletionException;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import static java.util.Collections.unmodifiableList;
import static java.util.stream.Collectors.toList;

public class InMemoryTopicRepository implements TopicRepository {

    public static final int DEFAULT_NUMBER_OF_PARTITIONS = 8;
    private final Map<String, MockTopic> topics;

    public InMemoryTopicRepository() {
        topics = new HashMap<>();
    }

    @Override
    public List<Topic> listTopics() throws NakadiException {
        return topics.values().stream().map(mockTopic -> new Topic(mockTopic.name)).collect(toList());
    }

    @Override
    public void createTopic(final String topic) {
        createTopic(topic, DEFAULT_NUMBER_OF_PARTITIONS);
    }

    public void createTopic(final String topicId, final int partitionsNum) {
        topics.put(topicId, new MockTopic(topicId, partitionsNum));
    }

    @Override
    public void createTopic(final String topic, final int partitionsNum, final int replicaFactor,
            final long retentionMs, final long rotationMs) {
        topics.put(topic, new MockTopic(topic, partitionsNum));
    }

    @Override
    public void deleteTopic(final String topic) throws TopicDeletionException {
        topics.remove(topic);
    }

    @Override
    public boolean topicExists(final String topic) throws NakadiException {
        throw new UnsupportedOperationException("not implemented");
    }

    @Override
    public boolean partitionExists(final String topic, final String partition) throws NakadiException {
        throw new UnsupportedOperationException("not implemented");
    }

    @Override
    public boolean areCursorsValid(final String topic, final List<Cursor> cursors) {
        throw new UnsupportedOperationException("not implemented");
    }

    @Override
    public void postEvent(final String topicId, final String partitionId, final String payload) throws NakadiException {
        getPartitionStorage(topicId, partitionId).postEvent(payload);
    }

    private MockPartition getPartitionStorage(final String topicId, final String partitionId) throws NakadiException {
        final MockTopic topic = topics.get(topicId);
        if (topic == null) {
            throw new InternalNakadiException("No such topic '" + topicId + "'");
        }

        final MockPartition partition = topic.partitions.get(partitionId);
        if (partition == null) {
            throw new InternalNakadiException("No such partition '" + partitionId + "'");
        }

        return partition;
    }

    @Override
    public List<TopicPartition> listPartitions(final String topicId) throws NakadiException {
        final MockTopic mockTopic = topics.get(topicId);
        if (mockTopic == null) {
            throw new InternalNakadiException("No such topic '" + topicId + "'");
        }

        return mockTopic.partitions.values().stream().map(p -> new TopicPartition(topicId, p.id)).collect(toList());
    }

    @Override
    public List<String> listPartitionNames(final String topicId) throws NakadiException {
        return unmodifiableList(listPartitions(topicId).stream().map(p -> p.getPartitionId()).collect(toList()));
    }

    @Override
    public TopicPartition getPartition(final String topicId, final String partition) throws NakadiException {
        throw new UnsupportedOperationException("not implemented");
    }

    @Override
    public EventConsumer createEventConsumer(final String topic, final Map<String, String> cursors) {
        throw new UnsupportedOperationException("Implement this method if needed");
    }

    public LinkedList<String> getEvents(final String topicId, final String partitionId) throws NakadiException {
        return getPartitionStorage(topicId, partitionId).events;
    }

    private static class MockTopic {
        private final String name;
        private final Map<String, MockPartition> partitions;

        private MockTopic(final String name, final int numberOfPartitions) {
            this.name = name;
            partitions = new HashMap<>();
            for (int i = 0; i < numberOfPartitions; i++) {
                final String partitionId = String.valueOf(i);
                partitions.put(partitionId, new MockPartition(partitionId));
            }
        }
    }

    private static class MockPartition {
        private final String id;
        private final LinkedList<String> events = new LinkedList<>();

        private MockPartition(final String id) {
            this.id = id;
        }

        public void postEvent(final String payload) {
            events.add(payload);
        }
    }
}
