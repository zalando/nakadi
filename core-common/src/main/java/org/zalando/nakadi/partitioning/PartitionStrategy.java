package org.zalando.nakadi.partitioning;

import org.json.JSONObject;
import org.zalando.nakadi.domain.EventType;
import org.zalando.nakadi.exceptions.runtime.PartitioningException;
import java.util.List;

public interface PartitionStrategy {

    String HASH_STRATEGY = "hash";
    String USER_DEFINED_STRATEGY = "user_defined";
    String RANDOM_STRATEGY = "random";

    String calculatePartition(PartitioningData partitioningData, List<String> partitions)
            throws PartitioningException;

    PartitioningData getDataFromJson(EventType eventType, JSONObject jsonEvent)
            throws PartitioningException;;
}
