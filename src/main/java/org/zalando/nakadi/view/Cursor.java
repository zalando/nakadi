package org.zalando.nakadi.view;

import com.fasterxml.jackson.annotation.JsonProperty;

import javax.annotation.concurrent.Immutable;
import javax.validation.constraints.NotNull;

@Immutable
public class Cursor {

    public static final String BEFORE_OLDEST_OFFSET = "BEGIN";

    @NotNull
    private final String partition;

    @NotNull
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

        if (partition != null ? !partition.equals(cursor.partition) : cursor.partition != null) {
            return false;
        }
        return !(offset != null ? !offset.equals(cursor.offset) : cursor.offset != null);
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
