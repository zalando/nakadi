package org.zalando.nakadi.view;

import com.fasterxml.jackson.annotation.JsonProperty;

import javax.validation.constraints.NotNull;

public class PartitionsNumberView {

    @NotNull
    private final Integer partitionsNumber;

    public PartitionsNumberView(@JsonProperty("partitions_number") final Integer partitionsNumber) {
        this.partitionsNumber = partitionsNumber;
    }

    public Integer getPartitionsNumber() {
        return partitionsNumber;
    }
}
