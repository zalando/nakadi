package org.zalando.nakadi.partitioning;

import com.google.common.base.Strings;
import org.json.JSONException;
import org.json.JSONObject;
import org.zalando.nakadi.domain.EventType;
import org.zalando.nakadi.domain.NakadiMetadata;
import org.zalando.nakadi.exceptions.runtime.PartitioningException;

import java.util.List;

public class UserDefinedPartitionStrategy implements PartitionStrategy {

    @Override
    public String calculatePartition(final EventType eventType,
                                     final JSONObject jsonEvent,
                                     final List<String> partitions)
            throws PartitioningException {
        final var userDefinedPartition = getPartitionFromMetadata(jsonEvent);

        return calculatePartition(userDefinedPartition, partitions);
    }

    @Override
    public String calculatePartition(final NakadiMetadata nakadiRecordMetadata,
                                     final List<String> partitions)
            throws PartitioningException {
        final var userDefinedPartition = nakadiRecordMetadata.getPartitionStr();

        return calculatePartition(userDefinedPartition, partitions);
    }

    public String calculatePartition(final String userDefinedPartition,
                                     final List<String> partitions)
            throws PartitioningException {
        if (Strings.isNullOrEmpty(userDefinedPartition)) {
            throw new PartitioningException("Failed to resolve partition. " +
                    "Failed to get partition from event metadata");
        }

        if (partitions.contains(userDefinedPartition)) {
            return userDefinedPartition;
        } else {
            throw new PartitioningException("Failed to resolve partition. " +
                    "Invalid partition specified when publishing event.");
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
