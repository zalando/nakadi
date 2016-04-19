package de.zalando.aruha.nakadi.repository.kafka;

import org.springframework.beans.factory.annotation.Value;

class KafkaRepositorySettings {

    @Value("${nakadi.topic.max.partitionNum}")
    private int maxTopicPartitionCount;

    @Value("${nakadi.topic.default.partitionNum}")
    private int defaultTopicPartitionCount;

    @Value("${nakadi.topic.default.replicaFactor}")
    private int defaultTopicReplicaFactor;

    @Value("${nakadi.topic.default.retentionMs}")
    private long defaultTopicRetentionMs;

    @Value("${nakadi.topic.default.rotationMs}")
    private long defaultTopicRotationMs;

    @Value("${nakadi.kafka.poll.timeoutMs}")
    private long kafkaPollTimeoutMs;

    @Value("${nakadi.kafka.send.timeoutMs}")
    private long kafkaSendTimeoutMs;

    @Value("${nakadi.zookeeper.sessionTimeoutMs}")
    private int zkSessionTimeoutMs;

    @Value("${nakadi.zookeeper.connectionTimeoutMs}")
    private int zkConnectionTimeoutMs;

    public int getDefaultTopicPartitionCount() {
        return defaultTopicPartitionCount;
    }

    public void setDefaultTopicPartitionCount(final int defaultTopicPartitionCount) {
        this.defaultTopicPartitionCount = defaultTopicPartitionCount;
    }

    public int getMaxTopicPartitionCount() {
        return maxTopicPartitionCount;
    }

    public void setMaxTopicPartitionCount(final int maxTopicPartitionCount) {
        this.maxTopicPartitionCount = maxTopicPartitionCount;
    }

    public int getDefaultTopicReplicaFactor() {
        return defaultTopicReplicaFactor;
    }

    public void setDefaultTopicReplicaFactor(final int defaultTopicReplicaFactor) {
        this.defaultTopicReplicaFactor = defaultTopicReplicaFactor;
    }

    public long getDefaultTopicRetentionMs() {
        return defaultTopicRetentionMs;
    }

    public void setDefaultTopicRetentionMs(final long defaultTopicRetentionMs) {
        this.defaultTopicRetentionMs = defaultTopicRetentionMs;
    }

    public long getDefaultTopicRotationMs() {
        return defaultTopicRotationMs;
    }

    public void setDefaultTopicRotationMs(final long defaultTopicRotationMs) {
        this.defaultTopicRotationMs = defaultTopicRotationMs;
    }

    public long getKafkaPollTimeoutMs() {
        return kafkaPollTimeoutMs;
    }

    public void setKafkaPollTimeoutMs(final long kafkaPollTimeoutMs) {
        this.kafkaPollTimeoutMs = kafkaPollTimeoutMs;
    }

    public long getKafkaSendTimeoutMs() {
        return kafkaSendTimeoutMs;
    }

    public void setKafkaSendTimeoutMs(final long kafkaSendTimeoutMs) {
        this.kafkaSendTimeoutMs = kafkaSendTimeoutMs;
    }

    public int getZkSessionTimeoutMs() {
        return zkSessionTimeoutMs;
    }

    public void setZkSessionTimeoutMs(final int zkSessionTimeoutMs) {
        this.zkSessionTimeoutMs = zkSessionTimeoutMs;
    }

    public int getZkConnectionTimeoutMs() {
        return zkConnectionTimeoutMs;
    }

    public void setZkConnectionTimeoutMs(final int zkConnectionTimeoutMs) {
        this.zkConnectionTimeoutMs = zkConnectionTimeoutMs;
    }
}
