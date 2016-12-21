package org.zalando.nakadi.partitioning;

import com.google.common.collect.ImmutableMap;
import org.zalando.nakadi.domain.EventType;
import org.zalando.nakadi.exceptions.InvalidEventTypeException;
import org.zalando.nakadi.exceptions.NoSuchPartitionStrategyException;
import org.zalando.nakadi.exceptions.PartitioningException;
import org.zalando.nakadi.repository.TopicRepository;
import org.json.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Map;
import java.util.Random;

import static com.google.common.collect.Lists.newArrayList;
import static org.zalando.nakadi.domain.EventCategory.UNDEFINED;
import static org.zalando.nakadi.partitioning.PartitionStrategy.HASH_STRATEGY;
import static org.zalando.nakadi.partitioning.PartitionStrategy.RANDOM_STRATEGY;
import static org.zalando.nakadi.partitioning.PartitionStrategy.USER_DEFINED_STRATEGY;

@Component
public class PartitionResolver {

    private static final Map<String, PartitionStrategy> PARTITION_STRATEGIES = ImmutableMap.of(
            HASH_STRATEGY, new HashPartitionStrategy(),
            USER_DEFINED_STRATEGY, new UserDefinedPartitionStrategy(),
            RANDOM_STRATEGY, new RandomPartitionStrategy(new Random())
    );

    public static final List<String> ALL_PARTITION_STRATEGIES = newArrayList(PARTITION_STRATEGIES.keySet());

    private final TopicRepository topicRepository;

    @Autowired
    public PartitionResolver(final TopicRepository topicRepository) {
        this.topicRepository = topicRepository;
    }

    public void validate(final EventType eventType) throws NoSuchPartitionStrategyException, InvalidEventTypeException {
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

    public String resolvePartition(final EventType eventType, final JSONObject eventAsJson, final List<String> partitions)
            throws PartitioningException {

        final String eventTypeStrategy = eventType.getPartitionStrategy();
        final PartitionStrategy partitionStrategy = PARTITION_STRATEGIES.get(eventTypeStrategy);
        if (partitionStrategy == null) {
            throw new PartitioningException("Partition Strategy defined for this EventType is not found: " +
                    eventTypeStrategy);
        }

        return partitionStrategy.calculatePartition(eventType, eventAsJson, partitions);
    }

}
