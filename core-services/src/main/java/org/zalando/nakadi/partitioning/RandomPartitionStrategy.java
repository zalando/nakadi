package org.zalando.nakadi.partitioning;

import org.zalando.nakadi.domain.BatchItem;
import org.zalando.nakadi.domain.NakadiMetadata;
import org.zalando.nakadi.exceptions.runtime.PartitioningException;

import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

public class RandomPartitionStrategy implements PartitionStrategy {

    @Override
    public String calculatePartition(final BatchItem item, final List<String> orderedPartitions)
            throws PartitioningException {

        return getRandomPartition(orderedPartitions);
    }

    @Override
    public String calculatePartition(final NakadiMetadata recordMetadata, final List<String> orderedPartitions)
            throws PartitioningException {

        return getRandomPartition(orderedPartitions);
    }

    String getRandomPartition(final List<String> partitions) {
        if (partitions.size() == 1) {
            return partitions.get(0);
        } else {
            final int partitionIndex = ThreadLocalRandom.current().nextInt(partitions.size());
            return partitions.get(partitionIndex);
        }
    }
}
