package org.zalando.nakadi.partitioning;

import org.json.JSONException;
import org.json.JSONObject;
import org.zalando.nakadi.domain.BatchItem;
import org.zalando.nakadi.domain.NakadiMetadata;
import org.zalando.nakadi.exceptions.runtime.PartitioningException;

import java.util.List;

public class UserDefinedPartitionStrategy implements PartitionStrategy {

    @Override
    public String calculatePartition(final BatchItem item, final List<String> orderedPartitions)
            throws PartitioningException {

        final String partition = getPartitionFromMetadata(item.getEvent());
        return resolvePartition(partition, orderedPartitions);
    }

    @Override
    public String calculatePartition(final NakadiMetadata recordMetadata, final List<String> orderedPartitions)
            throws PartitioningException {

        final String partition = recordMetadata.getPartition();
        return resolvePartition(partition, orderedPartitions);
    }

    String resolvePartition(final String userDefinedPartition, final List<String> partitions)
            throws PartitioningException {

        if (partitions.contains(userDefinedPartition)) {
            return userDefinedPartition;
        } else {
            throw new PartitioningException("Failed to resolve partition. " +
                    "Invalid partition specified when publishing event: " + userDefinedPartition);
        }
    }

    private String getPartitionFromMetadata(final JSONObject jsonEvent)
            throws PartitioningException {
        try {
            return jsonEvent.getJSONObject("metadata").getString("partition");
        } catch (JSONException e) {
            throw new PartitioningException("Failed to resolve partition. " +
                    "Failed to get partition from event metadata", e);
        }
    }
}
