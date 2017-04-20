package org.zalando.nakadi.domain;

import java.util.Objects;
import javax.annotation.concurrent.Immutable;

@Immutable
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

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof TopicPartition)) {
            return false;
        }
        final TopicPartition that = (TopicPartition) o;
        return Objects.equals(topic, that.topic) && Objects.equals(partition, that.partition);
    }

    @Override
    public int hashCode() {
        return Objects.hash(topic, partition);
    }
}
