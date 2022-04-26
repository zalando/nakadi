package org.zalando.nakadi.partitioning;

import org.json.JSONException;
import org.json.JSONObject;
import org.zalando.nakadi.domain.EventType;
import org.zalando.nakadi.exceptions.runtime.PartitioningException;
import org.zalando.nakadi.repository.kafka.KafkaCursor;

public class UserDefinedPartitionStrategy implements PartitionStrategy {

    @Override
    public int calculatePartition(final EventType eventType, final JSONObject event, final int partitionsNumber)
            throws PartitioningException {
        try {
            final int partition = KafkaCursor.toKafkaPartition(event.getJSONObject("metadata").getString("partition"));
            if (0 <= partition && partition < partitionsNumber) {
                return partition;
            } else {
                throw new PartitioningException("Failed to resolve partition. " +
                        "Invalid partition specified when publishing event.");
            }
        }
        catch (JSONException e) {
            throw new PartitioningException("Failed to resolve partition. " +
                    "Failed to get partition from event metadata", e);
        }
    }
}
