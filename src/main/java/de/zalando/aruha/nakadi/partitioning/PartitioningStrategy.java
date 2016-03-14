package de.zalando.aruha.nakadi.partitioning;

import de.zalando.aruha.nakadi.domain.EventType;
import de.zalando.aruha.nakadi.exceptions.PartitioningException;
import org.json.JSONObject;

import java.util.List;

public interface PartitioningStrategy {

    String PARTITIONING_KEYS_STRATEGY = "partitioning_keys";
    String DUMMY_STRATEGY = "dummy"; // todo: temporary, will be removed after "user defined" is implemented

    String calculatePartition(final EventType eventType, final JSONObject event, final List<String> partitions)
            throws PartitioningException;
}
