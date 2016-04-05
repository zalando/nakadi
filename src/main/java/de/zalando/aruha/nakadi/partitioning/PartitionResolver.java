package de.zalando.aruha.nakadi.partitioning;

import com.google.common.collect.ImmutableMap;
import de.zalando.aruha.nakadi.domain.EventType;
import de.zalando.aruha.nakadi.domain.PartitionStrategyDescriptor;
import de.zalando.aruha.nakadi.exceptions.InvalidEventTypeException;
import de.zalando.aruha.nakadi.exceptions.NoSuchPartitionStrategyException;
import de.zalando.aruha.nakadi.exceptions.PartitioningException;
import de.zalando.aruha.nakadi.repository.TopicRepository;
import org.json.JSONObject;

import java.util.List;
import java.util.Map;
import java.util.Random;

import static de.zalando.aruha.nakadi.domain.EventCategory.UNDEFINED;
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

    public void validate(final EventType eventType) throws NoSuchPartitionStrategyException, InvalidEventTypeException {
        final PartitionStrategyDescriptor partitionStrategy = eventType.getPartitionStrategy();

        if (!this.strategyExists(partitionStrategy.getName())) {
            throw new NoSuchPartitionStrategyException("partition strategy does not exist: " +
                    partitionStrategy.getName());
        } else if (HASH_STRATEGY.equals(partitionStrategy.getName()) &&
                   eventType.getPartitionKeyFields().isEmpty()) {
            throw new InvalidEventTypeException("partition_key_fields field should be set for " +
                    "partition strategy 'hash'");
        } else if (USER_DEFINED_STRATEGY.equals(partitionStrategy.getName()) &&
                   UNDEFINED.equals(eventType.getCategory())) {
            throw new InvalidEventTypeException("'user_defined' partition strategy can't be used " +
                    "for EventType of category 'undefined'");
        }
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
