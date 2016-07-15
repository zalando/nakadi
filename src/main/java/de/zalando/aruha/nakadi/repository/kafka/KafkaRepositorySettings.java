package de.zalando.aruha.nakadi.repository.kafka;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;

class KafkaRepositorySettings {

    private final int maxTopicPartitionCount;
    private final int defaultTopicPartitionCount;
    private final int defaultTopicReplicaFactor;
    private final long defaultTopicRetentionMs;
    private final long defaultTopicRotationMs;
    private final long kafkaPollTimeoutMs;
    private final long kafkaSendTimeoutMs;
    private final int zkSessionTimeoutMs;
    private final int zkConnectionTimeoutMs;

    @Autowired
    public KafkaRepositorySettings(@Value("${nakadi.topic.max.partitionNum}") final int maxTopicPartitionCount,
                                   @Value("${nakadi.topic.default.partitionNum}") final int defaultTopicPartitionCount,
                                   @Value("${nakadi.topic.default.replicaFactor}") final int defaultTopicReplicaFactor,
                                   @Value("${nakadi.topic.default.retentionMs}") final long defaultTopicRetentionMs,
                                   @Value("${nakadi.topic.default.rotationMs}") final long defaultTopicRotationMs,
                                   @Value("${nakadi.kafka.poll.timeoutMs}") final long kafkaPollTimeoutMs,
                                   @Value("${nakadi.kafka.send.timeoutMs}") final long kafkaSendTimeoutMs,
                                   @Value("${nakadi.zookeeper.sessionTimeoutMs}") final int zkSessionTimeoutMs,
                                   @Value("${nakadi.zookeeper.connectionTimeoutMs}")final int zkConnectionTimeoutMs)
    {
        this.maxTopicPartitionCount = maxTopicPartitionCount;
        this.defaultTopicPartitionCount = defaultTopicPartitionCount;
        this.defaultTopicReplicaFactor = defaultTopicReplicaFactor;
        this.defaultTopicRetentionMs = defaultTopicRetentionMs;
        this.defaultTopicRotationMs = defaultTopicRotationMs;
        this.kafkaPollTimeoutMs = kafkaPollTimeoutMs;
        this.kafkaSendTimeoutMs = kafkaSendTimeoutMs;
        this.zkSessionTimeoutMs = zkSessionTimeoutMs;
        this.zkConnectionTimeoutMs = zkConnectionTimeoutMs;
    }

    public int getDefaultTopicPartitionCount() {
        return defaultTopicPartitionCount;
    }

    public int getMaxTopicPartitionCount() {
        return maxTopicPartitionCount;
    }

    public int getDefaultTopicReplicaFactor() {
        return defaultTopicReplicaFactor;
    }

    public long getDefaultTopicRetentionMs() {
        return defaultTopicRetentionMs;
    }

    public long getDefaultTopicRotationMs() {
        return defaultTopicRotationMs;
    }

    public long getKafkaPollTimeoutMs() {
        return kafkaPollTimeoutMs;
    }

    public long getKafkaSendTimeoutMs() {
        return kafkaSendTimeoutMs;
    }

    public int getZkSessionTimeoutMs() {
        return zkSessionTimeoutMs;
    }

    public int getZkConnectionTimeoutMs() {
        return zkConnectionTimeoutMs;
    }

}
