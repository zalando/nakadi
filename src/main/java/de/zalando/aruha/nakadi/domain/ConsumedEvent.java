package de.zalando.aruha.nakadi.domain;

public class ConsumedEvent {

    private String event;
    private String nextOffset;
    private TopicPartition topicPartition;

    public ConsumedEvent(final String event, final TopicPartition topicPartition, final String nextOffset) {
        this.event = event;
        this.nextOffset = nextOffset;
        this.topicPartition = topicPartition;
    }

    public String getEvent() {
        return event;
    }

    public TopicPartition getTopicPartition() {
        return topicPartition;
    }

    public String getNextOffset() {
        return nextOffset;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        final ConsumedEvent that = (ConsumedEvent) o;

        return event.equals(that.event) && nextOffset.equals(that.nextOffset)
                && topicPartition.equals(that.topicPartition);
    }

    @Override
    public int hashCode() {
        int result = event.hashCode();
        result = 31 * result + nextOffset.hashCode();
        result = 31 * result + topicPartition.hashCode();
        return result;
    }
}
