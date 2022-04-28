package org.zalando.nakadi.partitioning;

import org.json.JSONObject;
import org.zalando.nakadi.domain.EventCategory;
import org.zalando.nakadi.domain.EventType;
import org.zalando.nakadi.domain.NakadiMetadata;
import org.zalando.nakadi.exceptions.Try;
import org.zalando.nakadi.exceptions.runtime.InvalidPartitionKeyFieldsException;
import org.zalando.nakadi.exceptions.runtime.JsonPathAccessException;
import org.zalando.nakadi.util.JsonPathAccess;

import java.util.List;
import java.util.stream.Collectors;

public class PartitionData {
    private String partition;

    public String getPartition() {
        return partition;
    }

    public List<String> getPartitionKeys() {
        return partitionKeys;
    }

    private List<String> partitionKeys;

    public PartitionData(final String partition, final List<String> partitionKeys) {
        this.partition = partition;
        this.partitionKeys = partitionKeys;
    }

    public static PartitionData fromJson(final EventType eventType, final JSONObject jsonEvent) {
        final String partition = jsonEvent.has("metadata") && jsonEvent.getJSONObject("metadata").has("partition")
                ? jsonEvent.getJSONObject("metadata").getString("partition")
                : null;
        List<String> partitionKeys = null;
        if (eventType != null) {
            final List<String> partitionKeyFields = eventType.getPartitionKeyFields();

            if (!partitionKeyFields.isEmpty()) {
                final JsonPathAccess traversableJsonEvent = new JsonPathAccess(jsonEvent);
                partitionKeys = partitionKeyFields.stream()
                        .map(pkf -> EventCategory.DATA.equals(eventType.getCategory()) ? EventType.DATA_PATH_PREFIX + pkf : pkf)
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
        }

        return new PartitionData(partition, partitionKeys);
    }

    public static PartitionData fromNakadiMetadata(final NakadiMetadata metadata) {
        return new PartitionData(metadata.getPartitionStr(), metadata.getPartitionKeys());
    }
}
