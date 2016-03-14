package de.zalando.aruha.nakadi.partitioning;

import de.zalando.aruha.nakadi.domain.EventType;
import org.json.JSONObject;

import java.util.List;

/**
 * todo: it's a temporary partitioning strategy that will be removed when "user defined" partitioning strategy is implemented
 * It exists because we need some default partitioning strategy
 */
public class DummyPartitioningStrategy implements PartitioningStrategy {

    @Override
    public String calculatePartition(final EventType eventType, final JSONObject event, final List<String> partitions) {
        return "0";
    }


}
