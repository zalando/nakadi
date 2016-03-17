package de.zalando.aruha.nakadi.partitioning;

import de.zalando.aruha.nakadi.domain.EventType;
import de.zalando.aruha.nakadi.exceptions.PartitioningException;
import org.json.JSONObject;

import java.util.List;

@FunctionalInterface
public interface PartitioningStrategy {

    String HASH_STRATEGY = "hash";
    String DUMMY_STRATEGY = "dummy"; // todo: temporary, will be removed after "random" is implemented

    String calculatePartition(final EventType eventType, final JSONObject event, final List<String> partitions)
            throws PartitioningException;
}
