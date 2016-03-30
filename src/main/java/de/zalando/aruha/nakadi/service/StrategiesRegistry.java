package de.zalando.aruha.nakadi.service;

import com.google.common.collect.ImmutableList;
import de.zalando.aruha.nakadi.domain.PartitionStrategyDescriptor;
import de.zalando.aruha.nakadi.partitioning.PartitionStrategy;

import java.util.List;

public class StrategiesRegistry {

    public static final PartitionStrategyDescriptor HASH_PARTITION_STRATEGY =
            new PartitionStrategyDescriptor("hash", "This strategy will use the event " +
            "field(s) defined in 'partition_key_fields' property of `EventType` as a source for a hash " +
            "function that will caclulate the partition where the event will be put. All events with the " +
            "same value in this field(s) will go to the same partition, and consequently be ordered.");

    public static final PartitionStrategyDescriptor USER_DEFINED_PARTITION_STRATEGY =
            new PartitionStrategyDescriptor(PartitionStrategy.USER_DEFINED_STRATEGY, "This strategy will use " +
            "'metadata'.'partition' property of incoming event to know the partition where to put the event. " +
            "This strategy can't be used for `EventType` of category 'undefined'");

    public static final PartitionStrategyDescriptor RANDOM_PARTITION_STRATEGY =
            new PartitionStrategyDescriptor(PartitionStrategy.RANDOM_STRATEGY, "This strategy will put the event " +
            "to a random partition. Use it only if your `EventType` has one partition or if you don't care " +
            "about ordering of events");

    public static final List<PartitionStrategyDescriptor> AVAILABLE_PARTITION_STRATEGIES = ImmutableList.of(
            HASH_PARTITION_STRATEGY,
            USER_DEFINED_PARTITION_STRATEGY,
            RANDOM_PARTITION_STRATEGY
    );
}
