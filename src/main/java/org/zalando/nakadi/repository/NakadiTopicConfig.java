package org.zalando.nakadi.repository;

import org.zalando.nakadi.domain.CleanupPolicy;

import java.util.Optional;

public class NakadiTopicConfig {

    private final int partitionCount;

    private final CleanupPolicy cleanupPolicy;

    private final Optional<Long> retentionTimeMs;

    public NakadiTopicConfig(final int partitionCount,
                             final CleanupPolicy cleanupPolicy,
                             final Optional<Long> retentionTimeMs) {
        this.partitionCount = partitionCount;
        this.cleanupPolicy = cleanupPolicy;
        this.retentionTimeMs = retentionTimeMs;
    }

    public int getPartitionCount() {
        return partitionCount;
    }

    public CleanupPolicy getCleanupPolicy() {
        return cleanupPolicy;
    }

    public Optional<Long> getRetentionTimeMs() {
        return retentionTimeMs;
    }

}
