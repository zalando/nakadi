package de.zalando.aruha.nakadi.service;

import com.google.common.collect.ImmutableList;
import de.zalando.aruha.nakadi.domain.PartitionStrategyDescriptor;
import de.zalando.aruha.nakadi.partitioning.PartitionStrategy;

import java.util.List;

public class StrategiesRegistry {

    public static final PartitionStrategyDescriptor HASH_PARTITION_STRATEGY =
            new PartitionStrategyDescriptor("hash");

    public static final PartitionStrategyDescriptor USER_DEFINED_PARTITION_STRATEGY =
            new PartitionStrategyDescriptor(PartitionStrategy.USER_DEFINED_STRATEGY);

    public static final PartitionStrategyDescriptor RANDOM_PARTITION_STRATEGY =
            new PartitionStrategyDescriptor(PartitionStrategy.RANDOM_STRATEGY);

    public static final List<PartitionStrategyDescriptor> AVAILABLE_PARTITION_STRATEGIES = ImmutableList.of(
            HASH_PARTITION_STRATEGY,
            USER_DEFINED_PARTITION_STRATEGY,
            RANDOM_PARTITION_STRATEGY
    );
}
