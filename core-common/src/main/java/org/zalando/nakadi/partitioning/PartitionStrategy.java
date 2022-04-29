package org.zalando.nakadi.partitioning;

import org.zalando.nakadi.exceptions.runtime.PartitioningException;
import java.util.List;

@FunctionalInterface
public interface PartitionStrategy {

    String HASH_STRATEGY = "hash";
    String USER_DEFINED_STRATEGY = "user_defined";
    String RANDOM_STRATEGY = "random";

    String calculatePartition(PartitioningData partitioningData, List<String> partitions)
            throws PartitioningException;
}
