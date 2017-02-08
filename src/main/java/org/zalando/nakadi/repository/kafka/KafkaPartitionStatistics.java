package org.zalando.nakadi.repository.kafka;

import org.zalando.nakadi.domain.NakadiCursor;
import org.zalando.nakadi.domain.PartitionStatistics;

public class KafkaPartitionStatistics extends PartitionStatistics {

    private final long firstOffset;
    private final long lastOffset;

    public KafkaPartitionStatistics(final String topic, final int partition, final long firstOffset,
                                    final long lastOffset) {
        super(topic, KafkaCursor.toNakadiPartition(partition));
        this.firstOffset = firstOffset;
        this.lastOffset = lastOffset;
    }

    @Override
    public NakadiCursor getFirst() {
        return new NakadiCursor(getTopic(), getPartition(), KafkaCursor.toNakadiOffset(firstOffset));
    }

    @Override
    public NakadiCursor getLast() {
        return new NakadiCursor(getTopic(), getPartition(), KafkaCursor.toNakadiOffset(lastOffset));
    }

    @Override
    public NakadiCursor getBeforeFirst() {
        return new NakadiCursor(getTopic(), getPartition(), KafkaCursor.toNakadiOffset(firstOffset - 1));
    }
}
