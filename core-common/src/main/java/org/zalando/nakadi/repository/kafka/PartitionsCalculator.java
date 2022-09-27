package org.zalando.nakadi.repository.kafka;

import org.zalando.nakadi.config.NakadiSettings;
import org.zalando.nakadi.domain.EventTypeStatistics;

public class PartitionsCalculator {
    private final int maxPartitionCount;
    private final int defaultPartitionCount;

    public PartitionsCalculator(final int defaultPartitionCount,
                                 final int maxPartitionCount) {
        this.defaultPartitionCount = defaultPartitionCount;
        this.maxPartitionCount = maxPartitionCount;
    }

    public int getBestPartitionsCount(final EventTypeStatistics stat) {
        if (null == stat || stat.getReadParallelism() <= 0 && stat.getWriteParallelism() <= 0) {
            return defaultPartitionCount;
        }
        final int maxPartitionsDueParallelism = Math.max(stat.getReadParallelism(), stat.getWriteParallelism());

        return Math.min(maxPartitionsDueParallelism, maxPartitionCount);
    }

    static PartitionsCalculator load(final NakadiSettings nakadiSettings){
        return new PartitionsCalculator(
                        nakadiSettings.getDefaultTopicPartitionCount(),
                        nakadiSettings.getMaxTopicPartitionCount());
    }
}
