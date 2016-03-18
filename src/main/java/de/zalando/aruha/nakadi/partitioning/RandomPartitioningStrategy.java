package de.zalando.aruha.nakadi.partitioning;

import de.zalando.aruha.nakadi.domain.EventType;
import de.zalando.aruha.nakadi.exceptions.ExceptionWrapper;
import de.zalando.aruha.nakadi.exceptions.InvalidPartitioningKeyFieldsException;
import de.zalando.aruha.nakadi.exceptions.PartitioningException;
import de.zalando.aruha.nakadi.util.JsonPathAccess;
import org.json.JSONObject;

import java.util.List;
import java.util.Random;

import static de.zalando.aruha.nakadi.exceptions.ExceptionWrapper.wrapFunction;
import static java.lang.Math.abs;

public class RandomPartitioningStrategy implements PartitioningStrategy {

    private static final Random random = new Random();

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
