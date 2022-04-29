package org.zalando.nakadi.partitioning;

import org.json.JSONObject;
import org.zalando.nakadi.domain.EventCategory;
import org.zalando.nakadi.domain.EventType;
import org.zalando.nakadi.domain.NakadiMetadata;
import org.zalando.nakadi.exceptions.Try;
import org.zalando.nakadi.exceptions.runtime.InvalidPartitionKeyFieldsException;
import org.zalando.nakadi.exceptions.runtime.JsonPathAccessException;
import org.zalando.nakadi.util.JsonPathAccess;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import static org.zalando.nakadi.partitioning.PartitionStrategy.HASH_STRATEGY;

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
