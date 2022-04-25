package org.zalando.nakadi.partitioning;

import org.apache.avro.generic.GenericRecord;
import org.json.JSONObject;
import org.zalando.nakadi.domain.EventCategory;
import org.zalando.nakadi.domain.EventType;
import org.zalando.nakadi.exceptions.Try;
import org.zalando.nakadi.exceptions.runtime.InvalidPartitionKeyFieldsException;
import org.zalando.nakadi.exceptions.runtime.JsonPathAccessException;
import org.zalando.nakadi.exceptions.runtime.PartitioningException;
import org.zalando.nakadi.util.JsonPathAccess;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@FunctionalInterface
public interface PartitionStrategy {

    String HASH_STRATEGY = "hash";
    String USER_DEFINED_STRATEGY = "user_defined";
    String RANDOM_STRATEGY = "random";

    String calculatePartition(EventType eventType, PartitionData event, List<String> partitions)
            throws PartitioningException;
}

class PartitionData {
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
        final String partition = jsonEvent.getJSONObject("metadata").getString("partition");
        final List<String> partitionKeyFields = eventType.getPartitionKeyFields();
        List<String> partitionKeys = null;
        if (!partitionKeyFields.isEmpty()) {
            final JsonPathAccess traversableJsonEvent = new JsonPathAccess(jsonEvent);
            partitionKeys = partitionKeyFields.stream()
                    .map(pkf -> EventCategory.DATA.equals(eventType.getCategory()) ? ".data" + pkf : pkf)
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
        return new PartitionData(partition, partitionKeys);
    }

    public static PartitionData fromAvro(final GenericRecord metadata) {
        final String partition = metadata.get("partition").toString();
        final List<String> partition_keys = (List<String>) Collections.singletonList(metadata.get("partition_keys"));

        return new PartitionData(partition, partition_keys);
    }
}