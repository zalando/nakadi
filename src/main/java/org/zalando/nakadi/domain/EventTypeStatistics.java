package org.zalando.nakadi.domain;

import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;

import lombok.Data;

@Data
public class EventTypeStatistics {
    @NotNull
    @Min(value = 0, message = "can't be less then zero")
    private Integer messagesPerMinute;
    @NotNull
    @Min(value = 1, message = "can't be less then 1")
    private Integer messageSize;
    @NotNull
    @Min(value = 1, message = "at least one reader expected")
    private Integer readParallelism;
    @NotNull
    @Min(value = 1, message = "at least one writer expected")
    private Integer writeParallelism;
}
