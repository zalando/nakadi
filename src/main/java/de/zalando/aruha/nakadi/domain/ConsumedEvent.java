package de.zalando.aruha.nakadi.domain;

public class ConsumedEvent {

    private String event;
    private String topic;
    private String partition;
    private String nextOffset;

    public ConsumedEvent(final String event, final String topic, final String partition, final String nextOffset) {
        this.event = event;
        this.topic = topic;
        this.partition = partition;
        this.nextOffset = nextOffset;
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

    public String getNextOffset() {
        return nextOffset;
    }
}
