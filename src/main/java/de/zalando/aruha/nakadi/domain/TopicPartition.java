package de.zalando.aruha.nakadi.domain;

public class TopicPartition implements Comparable<TopicPartition> {

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

    @Override
    public int compareTo(final TopicPartition tp) {
        final int topicCompare = this.topic.compareTo(tp.getTopic());
        if (topicCompare != 0) {
            return topicCompare;
        }
        else {
            return partition.compareTo(tp.getPartition());
        }
    }

    @Override
    public String toString() {
        return "TopicPartition{" +
                "topic='" + topic + '\'' +
                ", partition='" + partition + '\'' +
                '}';
    }
}
