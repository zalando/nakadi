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
    public String calculatePartition(final PartitioningData partitioningData, final List<String> partitions) {
        if (partitions.size() == 1) {
            return partitions.get(0);
        }
        else {
            final int partitionIndex = random.nextInt(partitions.size());
            return partitions.get(partitionIndex);
        }
    }

    @Override
    public PartitioningData getDataFromJson(EventType eventType, JSONObject jsonEvent) throws PartitioningException {
        return null;
    }
}
