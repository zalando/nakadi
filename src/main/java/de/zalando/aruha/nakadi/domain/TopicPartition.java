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

    @Override
    public boolean equals(final Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        final TopicPartition that = (TopicPartition) o;
        return topic.equals(that.topic) && partition.equals(that.partition);
    }

    @Override
    public int hashCode() {
        int result = topic.hashCode();
        result = 31 * result + partition.hashCode();
        return result;
    }
}
