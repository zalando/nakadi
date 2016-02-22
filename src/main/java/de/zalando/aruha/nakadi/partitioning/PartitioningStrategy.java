package de.zalando.aruha.nakadi.partitioning;

import de.zalando.aruha.nakadi.domain.EventType;
import org.json.JSONObject;

public interface PartitioningStrategy {
    String calculatePartition(final EventType eventType, final JSONObject event, final int numberOfPartitions);
}
