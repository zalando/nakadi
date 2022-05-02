package org.zalando.nakadi.partitioning;

import org.json.JSONObject;
import org.zalando.nakadi.domain.EventType;
import org.zalando.nakadi.exceptions.runtime.PartitioningException;

import java.util.List;
import java.util.Random;

public class RandomPartitionStrategy implements PartitionStrategy {

    private final Random random;

    public RandomPartitionStrategy(final Random random) {
        this.random = random;
    }

    @Override
    public String calculatePartition(final EventType eventType,
                                     final JSONObject jsonEvent,
                                     final List<String> partitions)
            throws PartitioningException {

        return getRandomPartition(partitions);
    }

    @Override
    public String calculatePartition(final PartitioningData partitioningData, final List<String> partitions) {
        return getRandomPartition(partitions);
    }

    public String calculatePartition(final List<String> partitions) {
        return getRandomPartition(partitions);
    }

    private String getRandomPartition(final List<String> partitions){
        if (partitions.size() == 1) {
            return partitions.get(0);
        } else {
            final int partitionIndex = random.nextInt(partitions.size());
            return partitions.get(partitionIndex);
        }
    }
}
