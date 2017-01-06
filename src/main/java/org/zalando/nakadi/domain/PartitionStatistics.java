package org.zalando.nakadi.domain;

public abstract class PartitionStatistics {
    private final String topic;
    private final String partition;

    public PartitionStatistics(final String topic, final String partition) {
        this.topic = topic;
        this.partition = partition;
    }

    public String getTopic() {
        return topic;
    }

    public String getPartition() {
        return partition;
    }

    public abstract TopicPosition getFirst();

    public abstract TopicPosition getLast();

    public abstract TopicPosition getBeforeFirst();
}
