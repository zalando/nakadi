package org.zalando.nakadi.partitioning;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.zalando.nakadi.domain.EventType;
import org.zalando.nakadi.domain.EventTypeBase;
import org.zalando.nakadi.exceptions.runtime.InvalidEventTypeException;
import org.zalando.nakadi.exceptions.runtime.NoSuchPartitionStrategyException;

import java.util.List;
import java.util.Map;
import java.util.Random;

import static org.zalando.nakadi.domain.EventCategory.UNDEFINED;
import static org.zalando.nakadi.partitioning.PartitionStrategy.HASH_STRATEGY;
import static org.zalando.nakadi.partitioning.PartitionStrategy.RANDOM_STRATEGY;
import static org.zalando.nakadi.partitioning.PartitionStrategy.USER_DEFINED_STRATEGY;

@Component
public class PartitionResolver {

    public static final List<String> ALL_PARTITION_STRATEGIES = ImmutableList.of(
            HASH_STRATEGY, USER_DEFINED_STRATEGY, RANDOM_STRATEGY);

    private final Map<String, PartitionStrategy> partitionStrategies;

    @Autowired
    public PartitionResolver(final HashPartitionStrategy hashPartitionStrategy) {
        partitionStrategies = ImmutableMap.of(
                HASH_STRATEGY, hashPartitionStrategy,
                USER_DEFINED_STRATEGY, new UserDefinedPartitionStrategy(),
                RANDOM_STRATEGY, new RandomPartitionStrategy(new Random())
        );
    }

    public void validate(final EventTypeBase eventType) throws NoSuchPartitionStrategyException,
            InvalidEventTypeException {
        final String partitionStrategy = eventType.getPartitionStrategy();

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

    public PartitionStrategy getPartitionStrategy(final EventType eventType)
            throws NoSuchPartitionStrategyException{

        final String eventTypeStrategy = eventType.getPartitionStrategy();
        final PartitionStrategy partitionStrategy = partitionStrategies.get(eventTypeStrategy);
        if (partitionStrategy == null) {
            throw new NoSuchPartitionStrategyException("Partition Strategy defined for this EventType is not found: " +
                    eventTypeStrategy);
        }
        return partitionStrategy;
    }
}
