package org.zalando.nakadi.partitioning;

import org.zalando.nakadi.domain.BatchItem;
import org.zalando.nakadi.domain.NakadiMetadata;
import org.zalando.nakadi.exceptions.runtime.PartitioningException;

import java.util.List;

public interface PartitionStrategy {

    String HASH_STRATEGY = "hash";
    String USER_DEFINED_STRATEGY = "user_defined";
    String RANDOM_STRATEGY = "random";

    String calculatePartition(BatchItem item, List<String> orderedPartitions)
            throws PartitioningException;

    String calculatePartition(NakadiMetadata recordMetadata, List<String> orderedPartitions)
            throws PartitioningException;
}
