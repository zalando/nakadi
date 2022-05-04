package org.zalando.nakadi.partitioning;

import java.util.List;

public class PartitioningData {

    private String partition;
    private List<String> partitionKeys;

    public PartitioningData() {
    }

    public String getPartition() {
        return partition;
    }

    public PartitioningData setPartition(final String partition) {
        this.partition = partition;
        return this;
    }

    public List<String> getPartitionKeys() {
        return partitionKeys;
    }

    public PartitioningData setPartitionKeys(final List<String> partitionKeys) {
        this.partitionKeys = partitionKeys;
        return this;
    }
}
