package org.zalando.nakadi.config;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

@Component
public class NakadiSettings {

    private final int maxTopicPartitionCount;
    private final int defaultTopicPartitionCount;
    private final int defaultTopicReplicaFactor;
    private final long defaultTopicRetentionMs;
    private final long defaultTopicRotationMs;
    private final long defaultCommitTimeoutSeconds;
    private final long kafkaPollTimeoutMs;
    private final long kafkaSendTimeoutMs;
    private final long publishTimeoutMs;
    private final long timelineWaitTimeoutMs;
    private final long eventMaxBytes;

    @Autowired
    public NakadiSettings(@Value("${nakadi.topic.max.partitionNum}") final int maxTopicPartitionCount,
                          @Value("${nakadi.topic.default.partitionNum}") final int defaultTopicPartitionCount,
                          @Value("${nakadi.topic.default.replicaFactor}") final int defaultTopicReplicaFactor,
                          @Value("${nakadi.topic.default.retentionMs}") final long defaultTopicRetentionMs,
                          @Value("${nakadi.topic.default.rotationMs}") final long defaultTopicRotationMs,
                          @Value("${nakadi.stream.default.commitTimeout}") final long defaultCommitTimeoutSeconds,
                          @Value("${nakadi.kafka.poll.timeoutMs}") final long kafkaPollTimeoutMs,
                          @Value("${nakadi.kafka.send.timeoutMs}") final long kafkaSendTimeoutMs,
                          @Value("${nakadi.publish.timeoutMs}") final long publishTimeoutMs,
                          @Value("${nakadi.timeline.wait.timeoutMs}") final long timelineWaitTimeoutMs,
                          @Value("${nakadi.event.max.bytes}") final long eventMaxBytes) {
        this.maxTopicPartitionCount = maxTopicPartitionCount;
        this.defaultTopicPartitionCount = defaultTopicPartitionCount;
        this.defaultTopicReplicaFactor = defaultTopicReplicaFactor;
        this.defaultTopicRetentionMs = defaultTopicRetentionMs;
        this.defaultTopicRotationMs = defaultTopicRotationMs;
        this.defaultCommitTimeoutSeconds = defaultCommitTimeoutSeconds;
        this.kafkaPollTimeoutMs = kafkaPollTimeoutMs;
        this.kafkaSendTimeoutMs = kafkaSendTimeoutMs;
        this.eventMaxBytes = eventMaxBytes;
        this.publishTimeoutMs = publishTimeoutMs;
        this.timelineWaitTimeoutMs = timelineWaitTimeoutMs;
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

    public long getDefaultCommitTimeoutSeconds() {
        return defaultCommitTimeoutSeconds;
    }

    public long getKafkaPollTimeoutMs() {
        return kafkaPollTimeoutMs;
    }

    public long getKafkaSendTimeoutMs() {
        return kafkaSendTimeoutMs;
    }

    public long getEventMaxBytes() {
        return eventMaxBytes;
    }

    public long getPublishTimeoutMs() {
        return publishTimeoutMs;
    }

    public long getTimelineWaitTimeoutMs() {
        return timelineWaitTimeoutMs;
    }
}
