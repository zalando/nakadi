package de.zalando.aruha.nakadi.partitioning;

import com.google.common.collect.ImmutableList;
import de.zalando.aruha.nakadi.domain.PartitionResolutionStrategy;

import java.util.List;

public class PartitioningStrategyRegistry {

    public static List<PartitionResolutionStrategy> AVAILABLE_PARTITIONING_STRATEGIES = ImmutableList.of(
            new PartitionResolutionStrategy(PartitioningStrategy.HASH_STRATEGY, ""),
            new PartitionResolutionStrategy(PartitioningStrategy.USER_DEFINED_STRATEGY, ""),
            new PartitionResolutionStrategy(PartitioningStrategy.RANDOM_STRATEGY, "")
    );
}
