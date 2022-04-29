package org.zalando.nakadi.partitioning;

import com.google.common.base.Strings;
import org.json.JSONException;
import org.json.JSONObject;
import org.zalando.nakadi.domain.EventType;
import org.zalando.nakadi.exceptions.runtime.PartitioningException;

import java.util.List;

public class UserDefinedPartitionStrategy implements PartitionStrategy {

    @Override
    public String calculatePartition(final PartitioningData partitioningData, final List<String> partitions)
            throws PartitioningException {
        if (Strings.isNullOrEmpty(partitioningData.getPartition())) {
            throw new PartitioningException("Failed to resolve partition. " +
                    "Failed to get partition from event metadata");
        }

        final String partition = partitioningData.getPartition();
        if (partitions.contains(partition)) {
            return partition;
        } else {
            throw new PartitioningException("Failed to resolve partition. " +
                    "Invalid partition specified when publishing event.");
        }
    }

    @Override
    public PartitioningData getDataFromJson(EventType eventType, JSONObject jsonEvent) throws PartitioningException {
        try {
            final String partition = jsonEvent.getJSONObject("metadata").getString("partition");

            return new PartitioningData(partition);
        }
        catch (JSONException e) {
            throw new PartitioningException("Failed to resolve partition. " +
                    "Failed to get partition from event metadata", e);
        }
    }
}
