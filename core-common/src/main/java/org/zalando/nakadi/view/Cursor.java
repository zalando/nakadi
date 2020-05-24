package org.zalando.nakadi.view;

import com.fasterxml.jackson.annotation.JsonProperty;

import javax.annotation.concurrent.Immutable;
import javax.validation.constraints.NotNull;
import javax.validation.constraints.Size;
import java.util.Objects;

@Immutable
public class Cursor {

    public static final String BEFORE_OLDEST_OFFSET = "BEGIN";

    @NotNull
    @Size(min = 1, message = "cursor partition cannot be empty")
    private final String partition;

    @NotNull
    @Size(min = 1, message = "cursor offset cannot be empty")
    private final String offset;

    public Cursor(@JsonProperty("partition") final String partition, @JsonProperty("offset") final String offset) {
        this.partition = partition;
        this.offset = offset;
    }

    public String getPartition() {
        return partition;
    }

    public String getOffset() {
        return offset;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        final Cursor cursor = (Cursor) o;

        if (!Objects.equals(partition, cursor.partition)) {
            return false;
        }
        return Objects.equals(offset, cursor.offset);
    }

    @Override
    public int hashCode() {
        int result = partition != null ? partition.hashCode() : 0;
        result = 31 * result + (offset != null ? offset.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "Cursor{" +
                "partition='" + partition + '\'' +
                ", offset='" + offset + '\'' +
                '}';
    }
}
