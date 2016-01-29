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
}
