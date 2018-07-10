package org.zalando.nakadi.repository.kafka;

import kafka.admin.RackAwareMode;

import java.util.Optional;

public class KafkaTopicConfig {

    private final String topicName;

    private final int partitionCount;

    private final int replicaFactor;

    private final String cleanupPolicy;

    private final long segmentMs;

    private final Optional<Long> retentionMs;

    private final Optional<Long> segmentBytes;

    private final Optional<Long> minCompactionLagMs;

    private final RackAwareMode rackAwareMode;

    public KafkaTopicConfig(final String topicName,
                            final int partitionCount,
                            final int replicaFactor,
                            final String cleanupPolicy,
                            final long segmentMs,
                            final Optional<Long> retentionMs,
                            final Optional<Long> segmentBytes,
                            final Optional<Long> minCompactionLagMs,
                            final RackAwareMode rackAwareMode) {
        this.topicName = topicName;
        this.partitionCount = partitionCount;
        this.replicaFactor = replicaFactor;
        this.cleanupPolicy = cleanupPolicy;
        this.segmentMs = segmentMs;
        this.retentionMs = retentionMs;
        this.segmentBytes = segmentBytes;
        this.minCompactionLagMs = minCompactionLagMs;
        this.rackAwareMode = rackAwareMode;
    }

    public String getTopicName() {
        return topicName;
    }

    public int getPartitionCount() {
        return partitionCount;
    }

    public int getReplicaFactor() {
        return replicaFactor;
    }

    public String getCleanupPolicy() {
        return cleanupPolicy;
    }

    public long getSegmentMs() {
        return segmentMs;
    }

    public Optional<Long> getRetentionMs() {
        return retentionMs;
    }

    public Optional<Long> getSegmentBytes() {
        return segmentBytes;
    }

    public Optional<Long> getMinCompactionLagMs() {
        return minCompactionLagMs;
    }

    public RackAwareMode getRackAwareMode() {
        return rackAwareMode;
    }

}
