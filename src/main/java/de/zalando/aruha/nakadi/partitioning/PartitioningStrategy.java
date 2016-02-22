package de.zalando.aruha.nakadi.partitioning;

import de.zalando.aruha.nakadi.domain.EventType;

public interface PartitioningStrategy {
    String calculatePartition(final EventType eventType, final String event, final int numberOfPartitions);
}
