package de.zalando.aruha.nakadi.partitioning;

import com.google.common.collect.ImmutableMap;
import de.zalando.aruha.nakadi.domain.EventType;
import de.zalando.aruha.nakadi.domain.PartitionResolutionStrategy;
import de.zalando.aruha.nakadi.exceptions.PartitioningException;
import de.zalando.aruha.nakadi.repository.TopicRepository;
import org.json.JSONObject;

import java.util.List;
import java.util.Map;
import java.util.Random;

import static de.zalando.aruha.nakadi.partitioning.PartitioningStrategy.HASH_STRATEGY;
import static de.zalando.aruha.nakadi.partitioning.PartitioningStrategy.RANDOM_STRATEGY;
import static de.zalando.aruha.nakadi.partitioning.PartitioningStrategy.USER_DEFINED_STRATEGY;

public class PartitionResolver {

    private static Map<String, PartitioningStrategy> PARTITIONING_STRATEGIES = ImmutableMap.of(
            HASH_STRATEGY, new HashPartitioningStrategy(),
            USER_DEFINED_STRATEGY, new UserDefinedPartitioningStrategy(),
            RANDOM_STRATEGY, new RandomPartitioningStrategy(new Random())
    );

    private final TopicRepository topicRepository;

    public PartitionResolver(final TopicRepository topicRepository) {
        this.topicRepository = topicRepository;
    }

    public boolean strategyExists(final String strategyName) {
        return PARTITIONING_STRATEGIES.containsKey(strategyName);
    }

    public String resolvePartition(final EventType eventType, final JSONObject eventAsJson)
            throws PartitioningException {

        final PartitionResolutionStrategy eventTypeStrategy = eventType.getPartitionResolutionStrategy();
        final PartitioningStrategy partitioningStrategy = PARTITIONING_STRATEGIES.get(eventTypeStrategy.getName());
        if (partitioningStrategy == null) {
            throw new PartitioningException("Partition Strategy defined for this EventType is not found: " +
                    eventTypeStrategy.getName());
        }

        final List<String> partitions = topicRepository.listPartitionNames(eventType.getName());
        return partitioningStrategy.calculatePartition(eventType, eventAsJson, partitions);
    }

}
