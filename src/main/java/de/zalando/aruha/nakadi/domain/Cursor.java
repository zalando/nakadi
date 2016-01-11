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
}
