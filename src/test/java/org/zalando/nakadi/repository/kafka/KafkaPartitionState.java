package org.zalando.nakadi.repository.kafka;

public class KafkaPartitionState {
    final String topic;
    final int partition;
    final long earliestOffset;
    final long latestOffset;

    KafkaPartitionState(final String topic, final int partition, final long earliestOffset, final long latestOffset) {
        this.topic = topic;
        this.partition = partition;
        this.earliestOffset = earliestOffset;
        this.latestOffset = latestOffset;
    }

}
