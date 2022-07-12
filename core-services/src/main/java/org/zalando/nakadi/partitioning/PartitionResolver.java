package org.zalando.nakadi.partitioning;

import org.json.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.zalando.nakadi.domain.CleanupPolicy;
import org.zalando.nakadi.domain.BatchItem;
import org.zalando.nakadi.domain.EventCategory;
import org.zalando.nakadi.domain.EventType;
import org.zalando.nakadi.domain.EventTypeBase;
import org.zalando.nakadi.domain.NakadiMetadata;
import org.zalando.nakadi.exceptions.Try;
import org.zalando.nakadi.exceptions.runtime.InvalidEventTypeException;
import org.zalando.nakadi.exceptions.runtime.InvalidPartitionKeyFieldsException;
import org.zalando.nakadi.exceptions.runtime.JsonPathAccessException;
import org.zalando.nakadi.exceptions.runtime.NoSuchPartitionStrategyException;
import org.zalando.nakadi.exceptions.runtime.PartitioningException;
import org.zalando.nakadi.util.JsonPathAccess;

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

    public static final List<String> ALL_PARTITION_STRATEGIES = List.of(
            HASH_STRATEGY, USER_DEFINED_STRATEGY, RANDOM_STRATEGY);

    private final Map<String, PartitionStrategy> partitionStrategies;

    @Autowired
    public PartitionResolver(final HashPartitionStrategy hashPartitionStrategy) {
        partitionStrategies = Map.of(
                HASH_STRATEGY, hashPartitionStrategy,
                USER_DEFINED_STRATEGY, new UserDefinedPartitionStrategy(),
                RANDOM_STRATEGY, new RandomPartitionStrategy(new Random())
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

    public static List<String> getKeyFieldsForExtraction(final EventType eventType) {

        final List<String> partitionKeyFields = eventType.getPartitionKeyFields();
        if (partitionKeyFields == null || partitionKeyFields.isEmpty()) {
            throw new PartitioningException("Cannot extract partition keys: partition key fields not set!");
        }

        return partitionKeyFields.stream()
                .map(pkf -> EventCategory.DATA.equals(eventType.getCategory())
                        ? EventType.DATA_PATH_PREFIX + pkf
                        : pkf)
                .collect(Collectors.toList());
    }

    public static List<String> extractPartitionKeys(final List<String> partitionKeyFields, final JSONObject jsonEvent)
            throws PartitioningException {

        final JsonPathAccess traversableJsonEvent = new JsonPathAccess(jsonEvent);
        return partitionKeyFields.stream()
                .map(Try.wrap(pkf -> {
                                    try {
                                        return traversableJsonEvent.get(pkf).toString();
                                    } catch (final JsonPathAccessException e) {
                                        throw new InvalidPartitionKeyFieldsException(e.getMessage());
                                    }
                                }))
                .map(Try::getOrThrow)
                .collect(Collectors.toList());
    }

    public static String getEventKey(final EventType eventType, final BatchItem item) {

        if (isCompactedEventType(eventType)) {
            return item.getEvent()
                    .getJSONObject("metadata")
                    .getString("partition_compaction_key");
        } else {
            final List<String> partitionKeys = item.getPartitionKeys();
            if (partitionKeys != null) {
                return getEventKeyFromPartitionKeys(partitionKeys);
            }
        }

        // that's fine, not all events get a key assigned to them
        return null;
    }

    public static String getEventKey(final EventType eventType, final NakadiMetadata metadata) {

        if (isCompactedEventType(eventType)) {
            return metadata.getPartitionCompactionKey();
        } else {
            final List<String> partitionKeys = metadata.getPartitionKeys();
            if (partitionKeys != null) {
                return getEventKeyFromPartitionKeys(partitionKeys);
            }
        }

        // that's fine, not all events get a key assigned to them
        return null;
    }

    private static boolean isCompactedEventType(final EventType eventType) {
        return eventType.getCleanupPolicy() == CleanupPolicy.COMPACT ||
                eventType.getCleanupPolicy() == CleanupPolicy.COMPACT_AND_DELETE;
    }

    private static String getEventKeyFromPartitionKeys(final List<String> partitionKeys) {
        return String.join(",", partitionKeys);
    }
}
