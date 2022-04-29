package org.zalando.nakadi.partitioning;

import org.zalando.nakadi.domain.NakadiMetadata;
import java.util.List;

public class PartitioningData {

    private String partition;
    private List<String> partitionKeys;

    public PartitioningData(final String partition) {
        this.partition = partition;
    }

    public PartitioningData(final List<String> partitionKeys) {
        this.partitionKeys = partitionKeys;
    }

    public PartitioningData(final String partition, final List<String> partitionKeys) {
        this.partition = partition;
        this.partitionKeys = partitionKeys;
    }

    public String getPartition() {
        return partition;
    }

    public List<String> getPartitionKeys() {
        return partitionKeys;
    }

    public static PartitioningData fromNakadiMetadata(final NakadiMetadata metadata) {
        return new PartitioningData(metadata.getPartitionStr(), metadata.getPartitionKeys());
    }
}
