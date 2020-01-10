package org.zalando.nakadi.repository.kafka;

import org.zalando.nakadi.domain.NakadiCursor;
import org.zalando.nakadi.domain.PartitionBaseStatisticsImpl;
import org.zalando.nakadi.domain.PartitionStatistics;
import org.zalando.nakadi.domain.Timeline;

public class KafkaPartitionStatistics extends PartitionBaseStatisticsImpl implements PartitionStatistics {

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

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final KafkaPartitionStatistics that = (KafkaPartitionStatistics) o;
        return first.equals(that.first) && last.equals(that.last) && getTimeline().equals(that.getTimeline())
                && getPartition().equals(that.getPartition());
    }

    @Override
    public int hashCode() {
        int result = first.hashCode();
        result = 31 * result + last.hashCode();
        result = 31 * result + getTimeline().hashCode();
        result = 31 * result + getPartition().hashCode();
        return result;
    }
}
