package org.zalando.nakadi.partitioning;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.zalando.nakadi.exceptions.Try;
import org.zalando.nakadi.exceptions.runtime.InvalidPartitionKeyFieldsException;
import org.zalando.nakadi.exceptions.runtime.NakadiRuntimeException;

import java.util.List;
import java.util.stream.Collectors;

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
    public String calculatePartition(final PartitionData partitionData, final List<String> partitions)
            throws InvalidPartitionKeyFieldsException {
        if (partitionData.getPartitionKeys().isEmpty()) {
            throw new RuntimeException("Applying " + this.getClass().getSimpleName() + " although event type " +
                    "has no partition key fields configured.");
        }

        try {
            final int hashValue = partitionData.getPartitionKeys().stream()
                    .map(Try.wrap(pkf -> stringHash.hashCode(pkf)))
                    .map(Try::getOrThrow)
                    .mapToInt(hc -> hc)
                    .sum();

            int partitionIndex = abs(hashValue % partitions.size());
            partitionIndex = hashPartitioningCrutch.adjustPartitionIndex(partitionIndex, partitions.size());

            final List<String> sortedPartitions = partitions.stream().sorted().collect(Collectors.toList());
            return sortedPartitions.get(partitionIndex);

        } catch (NakadiRuntimeException e) {
            final Exception original = e.getException();
            if (original instanceof InvalidPartitionKeyFieldsException) {
                throw (InvalidPartitionKeyFieldsException) original;
            } else {
                throw e;
            }
        }
    }

}
