package de.zalando.aruha.nakadi.partitioning;

import de.zalando.aruha.nakadi.domain.EventType;
import org.json.JSONObject;

import java.util.List;
import java.util.Random;

public class RandomPartitioningStrategy implements PartitioningStrategy {

    private final Random random;

    public RandomPartitioningStrategy(final Random random) {
        this.random = random;
    }

    @Override
    public String calculatePartition(final EventType eventType, final JSONObject event, final List<String> partitions) {
        if (partitions.size() == 1) {
            return partitions.get(0);
        }
        else {
            final int partitionIndex = random.nextInt(partitions.size());
            return partitions.get(partitionIndex);
        }
    }


}
