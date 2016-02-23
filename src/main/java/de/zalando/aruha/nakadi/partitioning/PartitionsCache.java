package de.zalando.aruha.nakadi.partitioning;

import de.zalando.aruha.nakadi.exceptions.NakadiException;
import de.zalando.aruha.nakadi.repository.TopicRepository;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class PartitionsCache {

    private final Map<String, String[]> partitionsPerEventType = new ConcurrentHashMap<>();

    private final TopicRepository topicRepository;

    public PartitionsCache(final TopicRepository topicRepository) {
        this.topicRepository = topicRepository;
    }

    public String[] getPartitionsFor(final String eventTypeName) throws NakadiException {
        String[] partitions = partitionsPerEventType.get(eventTypeName);

        if (partitions == null) {
            partitions = topicRepository.listPartitionNames(eventTypeName);
            partitionsPerEventType.put(eventTypeName, partitions);
        }

        return partitions;
    }

}
