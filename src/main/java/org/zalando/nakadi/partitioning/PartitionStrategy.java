package org.zalando.nakadi.partitioning;

import org.zalando.nakadi.domain.EventType;
import org.zalando.nakadi.exceptions.PartitioningException;
import org.json.JSONObject;

import java.util.List;

@FunctionalInterface
public interface PartitionStrategy {

    String HASH_STRATEGY = "hash";
    String USER_DEFINED_STRATEGY = "user_defined";
    String RANDOM_STRATEGY = "random";

    String calculatePartition(final EventType eventType, final JSONObject event, final List<String> partitions)
            throws PartitioningException;
}
