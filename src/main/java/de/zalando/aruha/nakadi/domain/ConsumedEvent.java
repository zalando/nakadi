package de.zalando.aruha.nakadi.domain;

public class ConsumedEvent {

    private String event;
    private String topic;
    private String partition;
    private String offset;

    public ConsumedEvent(final String event, final String topic, final String partition, final String offset) {
        this.event = event;
        this.topic = topic;
        this.partition = partition;
        this.offset = offset;
    }

    public String getEvent() {
        return event;
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
}
