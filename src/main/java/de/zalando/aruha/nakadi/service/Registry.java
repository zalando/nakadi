package de.zalando.aruha.nakadi.service;

import com.google.common.collect.ImmutableList;
import de.zalando.aruha.nakadi.domain.PartitionResolutionStrategy;
import de.zalando.aruha.nakadi.partitioning.PartitioningStrategy;

import java.util.List;

public class Registry {

    public static final List<PartitionResolutionStrategy> AVAILABLE_PARTITIONING_STRATEGIES = ImmutableList.of(

            new PartitionResolutionStrategy(PartitioningStrategy.HASH_STRATEGY, "This strategy will use the event " +
                    "field(s) defined in 'partitioning_key_fields' property of `EventType` as a source for a hash " +
                    "function that will caclulate the partition where the event will be put. All events with the " +
                    "same value in this field(s) will go to the same partition, and consequently be ordered."),

            new PartitionResolutionStrategy(PartitioningStrategy.USER_DEFINED_STRATEGY, "This strategy will use " +
                    "'metadata'.'partition' property of incoming event to know the partition where to put the event. " +
                    "This strategy can't be used for `EventType` of category 'undefined'"),

            new PartitionResolutionStrategy(PartitioningStrategy.RANDOM_STRATEGY, "This strategy will put the event " +
                    "to a random partition. Use it only if your `EventType` has one partition or if you don't care " +
                    "about ordering of events")
    );
}
