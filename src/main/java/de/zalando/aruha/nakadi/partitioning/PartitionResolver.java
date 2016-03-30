package de.zalando.aruha.nakadi.partitioning;

import com.google.common.collect.ImmutableMap;
import de.zalando.aruha.nakadi.domain.EventType;
import de.zalando.aruha.nakadi.domain.PartitionStrategyDescriptor;
import de.zalando.aruha.nakadi.exceptions.PartitioningException;
import de.zalando.aruha.nakadi.repository.TopicRepository;
import org.json.JSONObject;

import java.util.List;
import java.util.Map;
import java.util.Random;

import static de.zalando.aruha.nakadi.partitioning.PartitionStrategy.HASH_STRATEGY;
import static de.zalando.aruha.nakadi.partitioning.PartitionStrategy.RANDOM_STRATEGY;
import static de.zalando.aruha.nakadi.partitioning.PartitionStrategy.USER_DEFINED_STRATEGY;

public class PartitionResolver {

    private static Map<String, PartitionStrategy> PARTITION_STRATEGIES = ImmutableMap.of(
            HASH_STRATEGY, new HashPartitionStrategy(),
            USER_DEFINED_STRATEGY, new UserDefinedPartitionStrategy(),
            RANDOM_STRATEGY, new RandomPartitionStrategy(new Random())
    );

    private final TopicRepository topicRepository;

    public PartitionResolver(final TopicRepository topicRepository) {
        this.topicRepository = topicRepository;
    }

    public boolean strategyExists(final String strategyName) {
        return PARTITION_STRATEGIES.containsKey(strategyName);
    }

    public String resolvePartition(final EventType eventType, final JSONObject eventAsJson)
            throws PartitioningException {

        final PartitionStrategyDescriptor eventTypeStrategy = eventType.getPartitionStrategy();
        final PartitionStrategy partitionStrategy = PARTITION_STRATEGIES.get(eventTypeStrategy.getName());
        if (partitionStrategy == null) {
            throw new PartitioningException("Partition Strategy defined for this EventType is not found: " +
                    eventTypeStrategy.getName());
        }

        final List<String> partitions = topicRepository.listPartitionNames(eventType.getName());
        return partitionStrategy.calculatePartition(eventType, eventAsJson, partitions);
    }

}
