package org.zalando.nakadi.repository.kafka;

public class PartitionState {
    final String topic;
    final int partition;
    final long earliestOffset;
    final long latestOffset;

    PartitionState(final String topic, final int partition, final long earliestOffset, final long latestOffset) {
        this.topic = topic;
        this.partition = partition;
        this.earliestOffset = earliestOffset;
        this.latestOffset = latestOffset;
    }

}
