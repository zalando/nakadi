package de.zalando.aruha.nakadi.partitioning;

import de.zalando.aruha.nakadi.domain.EventType;
import org.json.JSONObject;

import java.util.List;

public class OrderingKeyFieldsPartitioningStrategy implements PartitioningStrategy {

    @Override
    public String calculatePartition(final EventType eventType, final JSONObject event, final int numberOfPartitions) {
        final List<String> orderingKeyFields = eventType.getOrderingKeyFields();

        int hashValue = orderingKeyFields.stream()
                // The problem is that JSONObject doesn't override hashCode(). Therefore convert it to a string first
                // and then use hashCode()
                .map(okf -> event.get(okf).toString().hashCode())
                .mapToInt(hc -> hc)
                .sum();

        return String.valueOf(hashValue % numberOfPartitions);
    }
}
