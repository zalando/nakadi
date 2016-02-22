package de.zalando.aruha.nakadi.partitioning;

import de.zalando.aruha.nakadi.domain.EventType;
import org.json.JSONObject;

import java.util.List;

public class OrderingKeyFieldsPartitioningStrategy implements PartitioningStrategy {
    @Override
    public String calculatePartition(final EventType eventType, final JSONObject event, final int numberOfPartitions) {
        final List<String> orderingKeyFields = eventType.getOrderingKeyFields();


        return "1";
    }
}
