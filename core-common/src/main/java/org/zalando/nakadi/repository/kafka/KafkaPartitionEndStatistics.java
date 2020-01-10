package org.zalando.nakadi.repository.kafka;

import org.zalando.nakadi.domain.NakadiCursor;
import org.zalando.nakadi.domain.PartitionBaseStatisticsImpl;
import org.zalando.nakadi.domain.PartitionEndStatistics;
import org.zalando.nakadi.domain.Timeline;

public class KafkaPartitionEndStatistics extends PartitionBaseStatisticsImpl implements PartitionEndStatistics {

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

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final KafkaPartitionEndStatistics that = (KafkaPartitionEndStatistics) o;
        return last.equals(that.last) && getTimeline().equals(that.getTimeline())
                && getPartition().equals(that.getPartition());
    }

    @Override
    public int hashCode() {
        int result = last.hashCode();
        result = 31 * result + getTimeline().hashCode();
        result = 31 * result + getPartition().hashCode();
        return result;
    }
}
