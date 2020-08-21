package org.zalando.nakadi.view;

import com.fasterxml.jackson.annotation.JsonProperty;

import javax.validation.constraints.NotNull;

public class PartitionCountView {

    @NotNull
    private final Integer partitionCount;

    public PartitionCountView(@JsonProperty("partition_count") final Integer partitionsNumber) {
        this.partitionCount = partitionsNumber;
    }

    public Integer getPartitionCount() {
        return partitionCount;
    }
}
