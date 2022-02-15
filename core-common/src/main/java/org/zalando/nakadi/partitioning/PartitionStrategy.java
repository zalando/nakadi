package org.zalando.nakadi.partitioning;

import org.json.JSONObject;
import org.zalando.nakadi.domain.EventType;
import org.zalando.nakadi.exceptions.runtime.PartitioningException;

import java.util.List;

@FunctionalInterface
public interface PartitionStrategy {

    String HASH_STRATEGY = "hash";
    String USER_DEFINED_STRATEGY = "user_defined";
    String RANDOM_STRATEGY = "random";

    default List<String> extractEventKeys(EventType eventType, JSONObject event)
            throws PartitioningException {
        throw new PartitioningException("Not supported by partition strategy.");
    }

    String calculatePartition(EventType eventType, JSONObject event, List<String> partitions)
            throws PartitioningException;
}
