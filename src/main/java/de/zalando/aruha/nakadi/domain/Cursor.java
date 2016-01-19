package de.zalando.aruha.nakadi.domain;

public class Cursor {

    private String topic;
    private String partition;
    private String offset;

    public Cursor() {
    }

    public Cursor(final String topic, final String partition, final String offset) {
        this.topic = topic;
        this.partition = partition;
        this.offset = offset;
    }

    public String getTopic() {
        return topic;
    }

    public String getPartition() {
        return partition;
    }

    public String getOffset() {
        return offset;
    }

    public void setTopic(final String topic) {
        this.topic = topic;
    }

    public void setPartition(final String partition) {
        this.partition = partition;
    }

    public void setOffset(final String offset) {
        this.offset = offset;
    }

    @Override
    public String toString() {
        return "Cursor{" +
                "topic='" + topic + '\'' +
                ", partition='" + partition + '\'' +
                ", offset='" + offset + '\'' +
                '}';
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        final Cursor cursor = (Cursor) o;
        return topic.equals(cursor.topic) && partition.equals(cursor.partition) && offset.equals(cursor.offset);
    }

    @Override
    public int hashCode() {
        int result = topic.hashCode();
        result = 31 * result + partition.hashCode();
        result = 31 * result + offset.hashCode();
        return result;
    }
}
