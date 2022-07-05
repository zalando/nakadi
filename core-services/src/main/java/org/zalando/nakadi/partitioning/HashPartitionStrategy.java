package org.zalando.nakadi.partitioning;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.zalando.nakadi.domain.BatchItem;
import org.zalando.nakadi.domain.NakadiMetadata;
import org.zalando.nakadi.exceptions.runtime.PartitioningException;

import java.util.List;

import static java.lang.Math.abs;

@Component
public class HashPartitionStrategy implements PartitionStrategy {

    private final HashPartitionStrategyCrutch hashPartitioningCrutch;
    private final StringHash stringHash;

    @Autowired
    public HashPartitionStrategy(final HashPartitionStrategyCrutch hashPartitioningCrutch,
                                 final StringHash stringHash) {
        this.hashPartitioningCrutch = hashPartitioningCrutch;
        this.stringHash = stringHash;
    }

    @Override
    public String calculatePartition(final BatchItem item, final List<String> orderedPartitions)
            throws PartitioningException {

        return calculatePartition(item.getPartitionKeys(), orderedPartitions);
    }

    @Override
    public String calculatePartition(final NakadiMetadata recordMetadata, final List<String> orderedPartitions)
            throws PartitioningException {

        return calculatePartition(recordMetadata.getPartitionKeys(), orderedPartitions);
    }

    String calculatePartition(final List<String> partitionKeys, final List<String> orderedPartitions)
            throws PartitioningException {

        if (partitionKeys == null || partitionKeys.isEmpty()) {
            throw new PartitioningException("Cannot calculate hash value for partitioning: partition keys not set.");
        }

        // don't be tempted to use streams API here
        int hashValue = 0;
        for (final String pk : partitionKeys) {
            hashValue += stringHash.hashCode(pk);
        }

        int partitionIndex = abs(hashValue % orderedPartitions.size());
        partitionIndex = hashPartitioningCrutch.adjustPartitionIndex(partitionIndex, orderedPartitions.size());

        return orderedPartitions.get(partitionIndex);
    }
}
