package de.zalando.aruha.nakadi.domain;

public class Cursor {

    private String partition;
    private String offset;

    public Cursor() {
    }

    public Cursor(final String partition, final String offset) {
        this.partition = partition;
        this.offset = offset;
    }

    public String getPartition() {
        return partition;
    }

    public void setPartition(final String partition) {
        this.partition = partition;
    }

    public String getOffset() {
        return offset;
    }

    public void setOffset(final String offset) {
        this.offset = offset;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        final Cursor cursor = (Cursor) o;

        if (partition != null ? !partition.equals(cursor.partition) : cursor.partition != null) return false;
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
