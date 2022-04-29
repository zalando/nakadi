package org.zalando.nakadi.partitioning;

import org.json.JSONObject;
import org.zalando.nakadi.domain.EventCategory;
import org.zalando.nakadi.domain.EventType;
import org.zalando.nakadi.domain.NakadiMetadata;
import org.zalando.nakadi.exceptions.Try;
import org.zalando.nakadi.exceptions.runtime.InvalidPartitionKeyFieldsException;
import org.zalando.nakadi.exceptions.runtime.JsonPathAccessException;
import org.zalando.nakadi.util.JsonPathAccess;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import static org.zalando.nakadi.partitioning.PartitionStrategy.HASH_STRATEGY;

public class PartitioningData {

    private String partition;
    private List<String> partitionKeys;

    public PartitioningData(final String partition, final List<String> partitionKeys) {
        this.partition = partition;
        this.partitionKeys = partitionKeys;
    }

    public String getPartition() {
        return partition;
    }

    public List<String> getPartitionKeys() {
        return partitionKeys;
    }

    public static PartitioningData fromJson(final EventType eventType, final JSONObject jsonEvent) {
        final String partition = tryGetPartition(jsonEvent);
        List<String> partitionKeys = getPartitionKeys(eventType, jsonEvent);

        return new PartitioningData(partition, partitionKeys);
    }

    private static String tryGetPartition(JSONObject jsonEvent) {
        return jsonEvent.has("metadata") && jsonEvent.getJSONObject("metadata").has("partition")
                ? jsonEvent.getJSONObject("metadata").getString("partition")
                : null;
    }

    private static List<String> getPartitionKeys(final EventType eventType, final JSONObject jsonEvent) {
        if (eventType.getPartitionStrategy().equals(HASH_STRATEGY)) {
            final List<String> partitionKeyFields = eventType.getPartitionKeyFields();
            final JsonPathAccess traversableJsonEvent = new JsonPathAccess(jsonEvent);
            return partitionKeyFields.stream()
                    .map(pkf -> EventCategory.DATA.equals(eventType.getCategory())
                            ? EventType.DATA_PATH_PREFIX + pkf
                            : pkf)
                    .map(Try.wrap(okf -> {
                        try {
                            return traversableJsonEvent.get(okf).toString();
                        } catch (final JsonPathAccessException e) {
                            throw new InvalidPartitionKeyFieldsException(e.getMessage());
                        }
                    }))
                    .map(Try::getOrThrow)
                    .collect(Collectors.toList());
        }

        return Collections.emptyList();
    }

    public static PartitioningData fromNakadiMetadata(final NakadiMetadata metadata) {
        return new PartitioningData(metadata.getPartitionStr(), metadata.getPartitionKeys());
    }
}
