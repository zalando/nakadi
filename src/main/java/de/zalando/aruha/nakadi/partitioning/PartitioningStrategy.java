package de.zalando.aruha.nakadi.partitioning;

import de.zalando.aruha.nakadi.domain.EventType;
import de.zalando.aruha.nakadi.exceptions.InvalidOrderingKeyFieldsException;
import org.json.JSONObject;

import java.util.List;

public interface PartitioningStrategy {
    String calculatePartition(final EventType eventType, final JSONObject event, final List<String> partitions) throws InvalidOrderingKeyFieldsException;
}
