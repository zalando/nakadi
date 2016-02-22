package de.zalando.aruha.nakadi.partitioning;

import de.zalando.aruha.nakadi.domain.EventType;

public class OrderingKeyFieldsPartitioningStrategy implements PartitioningStrategy {
    @Override
    public String calculatePartition(final EventType eventType, final String event, final int numberOfPartitions) {
        return "1";
    }
}
