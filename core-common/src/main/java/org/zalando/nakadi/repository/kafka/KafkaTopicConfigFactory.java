package org.zalando.nakadi.repository.kafka;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.zalando.nakadi.domain.CleanupPolicy;
import org.zalando.nakadi.exceptions.runtime.TopicConfigException;
import org.zalando.nakadi.repository.NakadiTopicConfig;
import org.zalando.nakadi.util.UUIDGenerator;

import java.util.HashMap;
import java.util.Map;

@Component
public class KafkaTopicConfigFactory {

    private final UUIDGenerator uuidGenerator;

    private final int defaultTopicReplicaFactor;
    private final long defaultTopicRotationMs;
    private final long compactedTopicRotationMs;
    private final long compactedTopicSegmentBytes;
    private final long compactedTopicCompactionLagMs;

    @Autowired
    public KafkaTopicConfigFactory(
            final UUIDGenerator uuidGenerator,
            @Value("${nakadi.topic.default.replicaFactor}") final int defaultTopicReplicaFactor,
            @Value("${nakadi.topic.default.rotationMs}") final long defaultTopicRotationMs,
            @Value("${nakadi.topic.compacted.rotationMs}") final long compactedTopicRotationMs,
            @Value("${nakadi.topic.compacted.segmentBytes}") final long compactedTopicSegmentBytes,
            @Value("${nakadi.topic.compacted.compactionLagMs}") final long compactedTopicCompactionLagMs) {
        this.uuidGenerator = uuidGenerator;
        this.defaultTopicReplicaFactor = defaultTopicReplicaFactor;
        this.defaultTopicRotationMs = defaultTopicRotationMs;
        this.compactedTopicRotationMs = compactedTopicRotationMs;
        this.compactedTopicSegmentBytes = compactedTopicSegmentBytes;
        this.compactedTopicCompactionLagMs = compactedTopicCompactionLagMs;
    }

    public void configureCleanupPolicy(final KafkaTopicConfigBuilder builder, final CleanupPolicy cleanupPolicy) {
        switch (cleanupPolicy) {
            case COMPACT_AND_DELETE:
                builder.withCleanupPolicy("compact,delete");
                configureCompactionParameters(builder);
                break;
            case COMPACT:
                builder.withCleanupPolicy("compact");
                configureCompactionParameters(builder);
                break;
            case DELETE:
                builder
                        .withCleanupPolicy("delete")
                        .withSegmentMs(defaultTopicRotationMs);
                break;
            default:
                throw new IllegalStateException("Cleanup Policy not implemented " + cleanupPolicy.toString());
        }
    }

    public KafkaTopicConfig createKafkaTopicConfig(final NakadiTopicConfig topicConfig) throws TopicConfigException {

        // set common values
        final KafkaTopicConfigBuilder configBuilder = KafkaTopicConfigBuilder.builder()
                .withTopicName(uuidGenerator.randomUUID().toString())
                .withPartitionCount(topicConfig.getPartitionCount())
                .withReplicaFactor(defaultTopicReplicaFactor);

        configureCleanupPolicy(configBuilder, topicConfig.getCleanupPolicy());

        if (topicConfig.getCleanupPolicy() != CleanupPolicy.COMPACT) {
            configureDeletionRetentionMs(topicConfig, configBuilder);
        }

        return configBuilder.build();
    }

    private void configureDeletionRetentionMs(final NakadiTopicConfig topicConfig,
                                              final KafkaTopicConfigBuilder configBuilder) {
        configBuilder
                .withRetentionMs(topicConfig.getRetentionTimeMs()
                        .orElseThrow(() -> new TopicConfigException("retention time should be specified " +
                                "for topic with cleanup policy 'delete' or 'compact_and_delete'")));
    }

    private void configureCompactionParameters(final KafkaTopicConfigBuilder configBuilder) {
        configBuilder
                .withSegmentMs(compactedTopicRotationMs)
                .withSegmentBytes(compactedTopicSegmentBytes)
                .withMinCompactionLagMs(compactedTopicCompactionLagMs);
    }

    public Map<String, String> createKafkaTopicLevelProperties(final KafkaTopicConfig kafkaTopicConfig) {
        final Map<String, String> topicConfig = new HashMap<>();

        topicConfig.put("segment.ms", Long.toString(kafkaTopicConfig.getSegmentMs()));
        topicConfig.put("cleanup.policy", kafkaTopicConfig.getCleanupPolicy());

        kafkaTopicConfig.getRetentionMs()
                .ifPresent(v -> topicConfig.put("retention.ms", Long.toString(v)));
        kafkaTopicConfig.getSegmentBytes()
                .ifPresent(v -> topicConfig.put("segment.bytes", Long.toString(v)));
        kafkaTopicConfig.getMinCompactionLagMs()
                .ifPresent(v -> topicConfig.put("min.compaction.lag.ms", Long.toString(v)));

        return topicConfig;
    }

}
