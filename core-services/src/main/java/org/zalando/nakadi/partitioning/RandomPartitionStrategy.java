package org.zalando.nakadi.partitioning;

import org.json.JSONObject;
import org.zalando.nakadi.domain.EventType;

import java.util.Random;

public class RandomPartitionStrategy implements PartitionStrategy {

    private final Random random;

    public RandomPartitionStrategy(final Random random) {
        this.random = random;
    }

    @Override
    public int calculatePartition(final EventType eventType, final JSONObject event, final int partitionsNumber) {
        if (partitionsNumber == 1) {
            return 0;
        }
        else {
            return random.nextInt(partitionsNumber);
        }
    }
}
