package org.zalando.nakadi.partitioning;

import org.json.JSONException;
import org.json.JSONObject;
import org.zalando.nakadi.exceptions.runtime.PartitioningException;

import java.util.List;

public class UserDefinedPartitionStrategy implements PartitionStrategy {

    @Override
    public String calculatePartition(
            final JSONObject event, final List<String> eventKeys, final List<String> partitions)
            throws PartitioningException {

        try {
            final String partition = event.getJSONObject("metadata").getString("partition");
            if (partitions.contains(partition)) {
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
