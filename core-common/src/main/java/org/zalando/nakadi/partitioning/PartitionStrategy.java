package org.zalando.nakadi.partitioning;

import org.json.JSONObject;
import org.zalando.nakadi.domain.EventType;
import org.zalando.nakadi.domain.NakadiMetadata;
import org.zalando.nakadi.exceptions.runtime.PartitioningException;
import java.util.List;

public interface PartitionStrategy {

    String HASH_STRATEGY = "hash";
    String USER_DEFINED_STRATEGY = "user_defined";
    String RANDOM_STRATEGY = "random";

    String calculatePartition(EventType eventType, JSONObject jsonEvent, List<String> orderedPartitions)
            throws PartitioningException;

    String calculatePartition(NakadiMetadata nakadiRecordMetadata, List<String> orderedPartitions)
            throws PartitioningException;
}
