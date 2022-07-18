package org.zalando.nakadi.partitioning;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.zalando.nakadi.domain.BatchItem;
import org.zalando.nakadi.domain.EventType;
import org.zalando.nakadi.domain.EventTypeBase;
import org.zalando.nakadi.domain.NakadiMetadata;
import org.zalando.nakadi.exceptions.runtime.InvalidEventTypeException;
import org.zalando.nakadi.exceptions.runtime.NoSuchPartitionStrategyException;
import org.zalando.nakadi.exceptions.runtime.PartitioningException;

import java.util.List;
import java.util.Map;

import static org.zalando.nakadi.domain.EventCategory.UNDEFINED;
import static org.zalando.nakadi.partitioning.PartitionStrategy.HASH_STRATEGY;
import static org.zalando.nakadi.partitioning.PartitionStrategy.RANDOM_STRATEGY;
import static org.zalando.nakadi.partitioning.PartitionStrategy.USER_DEFINED_STRATEGY;

@Component
public class PartitionResolver {

    public static final List<String> ALL_PARTITION_STRATEGIES = List.of(
            HASH_STRATEGY, USER_DEFINED_STRATEGY, RANDOM_STRATEGY);

    private final Map<String, PartitionStrategy> partitionStrategies;

    @Autowired
    public PartitionResolver(final HashPartitionStrategy hashPartitionStrategy) {
        partitionStrategies = Map.of(
                HASH_STRATEGY, hashPartitionStrategy,
                USER_DEFINED_STRATEGY, new UserDefinedPartitionStrategy(),
                RANDOM_STRATEGY, new RandomPartitionStrategy()
        );
    }

    public void validate(final EventTypeBase eventType) throws NoSuchPartitionStrategyException,
            InvalidEventTypeException {

        final String partitionStrategy = eventType.getPartitionStrategy();
        if (null == partitionStrategy) {
            throw new InvalidEventTypeException("partition strategy must not be null");
        }
        if (!ALL_PARTITION_STRATEGIES.contains(partitionStrategy)) {
            throw new NoSuchPartitionStrategyException("partition strategy does not exist: " + partitionStrategy);
        } else if (HASH_STRATEGY.equals(partitionStrategy) && eventType.getPartitionKeyFields().isEmpty()) {
            throw new InvalidEventTypeException("partition_key_fields field should be set for " +
                    "partition strategy 'hash'");
        } else if (USER_DEFINED_STRATEGY.equals(partitionStrategy) && UNDEFINED.equals(eventType.getCategory())) {
            throw new InvalidEventTypeException("'user_defined' partition strategy can't be used " +
                    "for EventType of category 'undefined'");
        }
    }

    public String resolvePartition(final EventType eventType, final BatchItem item,
                                   final List<String> orderedPartitions)
            throws PartitioningException {

        return getPartitionStrategy(eventType).calculatePartition(item, orderedPartitions);
    }

    public String resolvePartition(final EventType eventType, final NakadiMetadata recordMetadata,
                                   final List<String> orderedPartitions)
            throws PartitioningException {

        return getPartitionStrategy(eventType).calculatePartition(recordMetadata, orderedPartitions);
    }

    private PartitionStrategy getPartitionStrategy(final EventType eventType) throws PartitioningException {

        final String eventTypeStrategy = eventType.getPartitionStrategy();
        final PartitionStrategy partitionStrategy = partitionStrategies.get(eventTypeStrategy);
        if (partitionStrategy == null) {
            throw new PartitioningException("Partition Strategy defined for this EventType is not found: " +
                    eventTypeStrategy);
        }
        return partitionStrategy;
    }

}
