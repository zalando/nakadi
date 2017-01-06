package org.zalando.nakadi.repository.kafka;

import org.zalando.nakadi.domain.TopicPosition;
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
    public TopicPosition getFirst() {
        return new TopicPosition(getTopic(), getPartition(), KafkaCursor.toNakadiOffset(firstOffset));
    }

    @Override
    public TopicPosition getLast() {
        return new TopicPosition(getTopic(), getPartition(), KafkaCursor.toNakadiOffset(lastOffset));
    }

    @Override
    public TopicPosition getBeforeFirst() {
        return new TopicPosition(getTopic(), getPartition(), KafkaCursor.toNakadiOffset(firstOffset - 1));
    }
}
