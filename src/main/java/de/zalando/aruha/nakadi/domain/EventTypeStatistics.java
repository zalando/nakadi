package de.zalando.aruha.nakadi.domain;

import java.util.Objects;
import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;

public class EventTypeStatistics {
    @NotNull
    @Min(value = 0, message = "expected_write_rate can't be less then zero")
    private Integer expectedWriteRate; // messages per minute
    @NotNull
    @Min(value = 1, message = "message_size can't be less then 1")
    private Integer messageSize; // bytes
    @NotNull
    @Min(value = 1, message = "problem with read_parallelism: at least one reader expected")
    private Integer readParallelism;
    @NotNull
    @Min(value = 1, message = "problem with write_parallelism: at least one writer expected")
    private Integer writeParallelism;

    public Integer getExpectedWriteRate() {
        return expectedWriteRate;
    }

    public void setExpectedWriteRate(Integer expectedWriteRate) {
        this.expectedWriteRate = expectedWriteRate;
    }

    public Integer getMessageSize() {
        return messageSize;
    }

    public void setMessageSize(Integer messageSize) {
        this.messageSize = messageSize;
    }

    public Integer getReadParallelism() {
        return readParallelism;
    }

    public void setReadParallelism(Integer readParallelism) {
        this.readParallelism = readParallelism;
    }

    public Integer getWriteParallelism() {
        return writeParallelism;
    }

    public void setWriteParallelism(Integer writeParallelism) {
        this.writeParallelism = writeParallelism;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        EventTypeStatistics that = (EventTypeStatistics) o;
        return Objects.equals(expectedWriteRate, that.expectedWriteRate)
                && Objects.equals(messageSize, that.messageSize)
                && Objects.equals(readParallelism, that.readParallelism)
                && Objects.equals(writeParallelism, that.writeParallelism);
    }

    @Override
    public int hashCode() {
        int result = expectedWriteRate != null ? expectedWriteRate.hashCode() : 0;
        result = 31 * result + (messageSize != null ? messageSize.hashCode() : 0);
        result = 31 * result + (readParallelism != null ? readParallelism.hashCode() : 0);
        result = 31 * result + (writeParallelism != null ? writeParallelism.hashCode() : 0);
        return result;
    }
}
