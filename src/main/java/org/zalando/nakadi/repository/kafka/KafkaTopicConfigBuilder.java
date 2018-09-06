package org.zalando.nakadi.repository.kafka;

import kafka.admin.RackAwareMode;

import java.util.Optional;

public final class KafkaTopicConfigBuilder {

    private String topicName;
    private int partitionCount;
    private int replicaFactor;
    private String cleanupPolicy;
    private long segmentMs;
    private Long retentionMs;
    private Long segmentBytes;
    private Long minCompactionLagMs;
    private RackAwareMode rackAwareMode;

    private KafkaTopicConfigBuilder() {
    }

    public static KafkaTopicConfigBuilder builder() {
        return new KafkaTopicConfigBuilder();
    }

    public KafkaTopicConfigBuilder withTopicName(final String topicName) {
        this.topicName = topicName;
        return this;
    }

    public KafkaTopicConfigBuilder withPartitionCount(final int partitionCount) {
        this.partitionCount = partitionCount;
        return this;
    }

    public KafkaTopicConfigBuilder withReplicaFactor(final int replicaFactor) {
        this.replicaFactor = replicaFactor;
        return this;
    }

    public KafkaTopicConfigBuilder withCleanupPolicy(final String cleanupPolicy) {
        this.cleanupPolicy = cleanupPolicy;
        return this;
    }

    public KafkaTopicConfigBuilder withSegmentMs(final long segmentMs) {
        this.segmentMs = segmentMs;
        return this;
    }

    public KafkaTopicConfigBuilder withRetentionMs(final Long retentionMs) {
        this.retentionMs = retentionMs;
        return this;
    }

    public KafkaTopicConfigBuilder withSegmentBytes(final Long segmentBytes) {
        this.segmentBytes = segmentBytes;
        return this;
    }

    public KafkaTopicConfigBuilder withMinCompactionLagMs(final Long minCompactionLagMs) {
        this.minCompactionLagMs = minCompactionLagMs;
        return this;
    }

    public KafkaTopicConfigBuilder withRackAwareMode(final RackAwareMode rackAwareMode) {
        this.rackAwareMode = rackAwareMode;
        return this;
    }

    public KafkaTopicConfig build() {
        return new KafkaTopicConfig(
                topicName,
                partitionCount,
                replicaFactor,
                cleanupPolicy,
                segmentMs,
                Optional.ofNullable(retentionMs),
                Optional.ofNullable(segmentBytes),
                Optional.ofNullable(minCompactionLagMs),
                rackAwareMode);
    }

}
