package de.zalando.aruha.nakadi.domain;

public class TopicPartition {

    private final String topic;

    private final String partition;

    public TopicPartition(final String topic, final String partition) {
        this.topic = topic;
        this.partition = partition;
    }

    public String getTopic() {
        return topic;
    }

    public String getPartition() {
        return partition;
    }
}
