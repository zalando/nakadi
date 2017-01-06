package org.zalando.nakadi.domain;

import java.util.Objects;

public class TopicPosition {
    private final String topic;
    private final String partition;
    // NO BEGIN OR END HERE!
    private final String offset;

    public TopicPosition(final String topic, final String partition, final String offset) {
        assert null != topic && null != partition && null != offset;
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

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof TopicPosition)) {
            return false;
        }

        final TopicPosition that = (TopicPosition) o;
        return Objects.equals(this.topic, that.topic)
                && Objects.equals(this.partition, that.partition)
                && Objects.equals(this.offset, that.offset);
    }

    @Override
    public int hashCode() {
        int result = topic.hashCode();
        result = 31 * result + partition.hashCode();
        result = 31 * result + offset.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return "TopicPosition{" +
                "topic='" + topic + '\'' +
                ", partition='" + partition + '\'' +
                ", offset='" + offset + '\'' +
                '}';
    }
}
