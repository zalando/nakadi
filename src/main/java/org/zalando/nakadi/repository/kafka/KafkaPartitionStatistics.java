package org.zalando.nakadi.repository.kafka;

import org.zalando.nakadi.domain.NakadiCursor;
import org.zalando.nakadi.domain.PartitionStatistics;
import org.zalando.nakadi.domain.Timeline;

public class KafkaPartitionStatistics extends PartitionStatistics {

    private final KafkaCursor first;
    private final KafkaCursor last;

    public KafkaPartitionStatistics(final Timeline timeline, final int partition, final long firstOffset,
                                    final long lastOffset) {
        super(timeline, KafkaCursor.toNakadiPartition(partition));
        this.first = new KafkaCursor(timeline.getTopic(), partition, firstOffset);
        this.last = new KafkaCursor(timeline.getTopic(), partition, lastOffset);
    }

    @Override
    public NakadiCursor getFirst() {
        // TODO: Support several timelines
        return first.toNakadiCursor(getTimeline());
    }

    @Override
    public NakadiCursor getLast() {
        // TODO: Support several timelines
        return last.toNakadiCursor(getTimeline());
    }

    @Override
    public NakadiCursor getBeforeFirst() {
        // TODO: Support several timelines
        return first.addOffset(-1).toNakadiCursor(getTimeline());
    }
}
