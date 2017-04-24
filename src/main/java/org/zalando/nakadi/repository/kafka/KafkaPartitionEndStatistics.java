package org.zalando.nakadi.repository.kafka;

import org.zalando.nakadi.domain.NakadiCursor;
import org.zalando.nakadi.domain.PartitionEndStatistics;
import org.zalando.nakadi.domain.Timeline;

public class KafkaPartitionEndStatistics extends PartitionEndStatistics {

    private final KafkaCursor last;

    public KafkaPartitionEndStatistics(final Timeline timeline, final int partition, final long lastOffset) {
        super(timeline, KafkaCursor.toNakadiPartition(partition));
        this.last = new KafkaCursor(timeline.getTopic(), partition, lastOffset);
    }

    @Override
    public NakadiCursor getLast() {
        // TODO: Support several timelines
        return last.toNakadiCursor(getTimeline());
    }

}
