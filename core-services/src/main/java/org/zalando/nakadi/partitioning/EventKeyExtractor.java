package org.zalando.nakadi.partitioning;

import org.json.JSONObject;
import org.zalando.nakadi.domain.BatchItem;
import org.zalando.nakadi.domain.CleanupPolicy;
import org.zalando.nakadi.domain.EventCategory;
import org.zalando.nakadi.domain.EventType;
import org.zalando.nakadi.domain.NakadiMetadata;
import org.zalando.nakadi.exceptions.Try;
import org.zalando.nakadi.exceptions.runtime.InvalidPartitionKeyFieldsException;
import org.zalando.nakadi.exceptions.runtime.JsonPathAccessException;
import org.zalando.nakadi.exceptions.runtime.PartitioningException;
import org.zalando.nakadi.util.JsonPathAccess;

import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Utility class to work with keys for partitioning/kafka keys.
 * <p>
 * Each record in kafka may have a key, that is used for compaction and joins in kstreams.
 * The calculation of the key is based either on partition_compaction_key from event (for log-compacted event types) or
 * on a set of fields used for partitioning. For now the set of fields used for partitioning may be extracted only in
 * case of hash partitioning.
 */
public class EventKeyExtractor {

    public static Function<BatchItem, List<String>> partitionKeysFromBatchItem(final EventType et) {
        if (PartitionStrategy.HASH_STRATEGY.equals(et.getPartitionStrategy())) {
            final List<String> fieldNames = getKeyFieldsForExtraction(et);
            return batchItem -> extractPartitionKeys(fieldNames, batchItem.getEvent());
        } else {
            return batchItem -> null;
        }
    }

    public static Function<BatchItem, String> kafkaKeyFromBatchItem(final EventType et) {
        if (isCompactedEventType(et)) {
            return batchItem -> batchItem.getEvent().getJSONObject("metadata").getString("partition_compaction_key");
        } else {
            return batchItem -> Optional.ofNullable(batchItem.getPartitionKeys())
                    .map(EventKeyExtractor::getKafkaKeyFromPartitionKeys)
                    .orElse(null);
        }
    }

    public static Function<NakadiMetadata, String> kafkaKeyFromNakadiMetadata(final EventType et) {
        if (isCompactedEventType(et)) {
            return NakadiMetadata::getPartitionCompactionKey;
        } else {
            return nakadiMetadata -> Optional.ofNullable(nakadiMetadata.getPartitionKeys())
                    .map(EventKeyExtractor::getKafkaKeyFromPartitionKeys)
                    .orElse(null);
        }
    }

    private static boolean isCompactedEventType(final EventType eventType) {
        return eventType.getCleanupPolicy() == CleanupPolicy.COMPACT ||
                eventType.getCleanupPolicy() == CleanupPolicy.COMPACT_AND_DELETE;
    }

    private static String getKafkaKeyFromPartitionKeys(final List<String> partitionKeys) {
        return String.join(",", partitionKeys);

    }

    private static List<String> getKeyFieldsForExtraction(final EventType eventType) {

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

    private static List<String> extractPartitionKeys(final List<String> partitionKeyFields, final JSONObject jsonEvent)
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

}
