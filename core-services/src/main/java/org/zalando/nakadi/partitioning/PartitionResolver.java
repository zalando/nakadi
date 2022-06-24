package org.zalando.nakadi.partitioning;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.json.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.zalando.nakadi.domain.EventType;
import org.zalando.nakadi.domain.EventTypeBase;
import org.zalando.nakadi.domain.NakadiMetadata;
import org.zalando.nakadi.exceptions.runtime.InvalidEventTypeException;
import org.zalando.nakadi.exceptions.runtime.NoSuchPartitionStrategyException;
import org.zalando.nakadi.exceptions.runtime.PartitioningException;
import org.zalando.nakadi.service.timeline.TimelineService;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.stream.Collectors;

import static org.zalando.nakadi.domain.EventCategory.UNDEFINED;
import static org.zalando.nakadi.partitioning.PartitionStrategy.HASH_STRATEGY;
import static org.zalando.nakadi.partitioning.PartitionStrategy.RANDOM_STRATEGY;
import static org.zalando.nakadi.partitioning.PartitionStrategy.USER_DEFINED_STRATEGY;

@Component
public class PartitionResolver {

    public static final List<String> ALL_PARTITION_STRATEGIES = ImmutableList.of(
            HASH_STRATEGY, USER_DEFINED_STRATEGY, RANDOM_STRATEGY);

    private final Map<String, PartitionStrategy> partitionStrategies;
    private final TimelineService timelineService;

    @Autowired
    public PartitionResolver(final TimelineService timelineService, final HashPartitionStrategy hashPartitionStrategy) {
        this.timelineService = timelineService;

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

    /**
     * Returns a sorted, unmodifiable list with fast access by index.
     */
    public List<String> getSortedPartitions(final EventType eventType) {
        final List<String> sortedPartitions = timelineService.getTopicRepository(eventType)
                .listPartitionNames(timelineService.getActiveTimeline(eventType).getTopic())
                .stream()
                .sorted()
                .collect(Collectors.toCollection(ArrayList::new));

        return Collections.unmodifiableList(sortedPartitions);
    }

    public String resolvePartition(final EventType eventType, final JSONObject eventAsJson,
            final List<String> sortedPartitions)
            throws PartitioningException {

        return getPartitionStrategy(eventType).calculatePartition(eventType, eventAsJson, sortedPartitions);
    }

    public String resolvePartition(final EventType eventType, final NakadiMetadata nakadiRecordMetadata,
            final List<String> sortedPartitions)
            throws PartitioningException {

        return getPartitionStrategy(eventType).calculatePartition(nakadiRecordMetadata, sortedPartitions);
    }

    private PartitionStrategy getPartitionStrategy(final EventType eventType) {
        final String eventTypeStrategy = eventType.getPartitionStrategy();
        final PartitionStrategy partitionStrategy = partitionStrategies.get(eventTypeStrategy);

        if (partitionStrategy == null) {
            throw new PartitioningException("Partition Strategy defined for this EventType is not found: " +
                    eventTypeStrategy);
        }

        return partitionStrategy;
    }
}
