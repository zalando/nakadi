package org.zalando.nakadi.repository.kafka;

import org.zalando.nakadi.domain.NakadiCursor;
import org.zalando.nakadi.domain.PartitionBaseStatisticsImpl;
import org.zalando.nakadi.domain.PartitionStartStatistics;
import org.zalando.nakadi.domain.Timeline;

public class KafkaPartitionStartStatistics extends PartitionBaseStatisticsImpl implements PartitionStartStatistics {

    private final KafkaCursor first;

    public KafkaPartitionStartStatistics(final Timeline timeline, final int partition, final long firstOffset) {
        super(timeline, KafkaCursor.toNakadiPartition(partition));
        this.first = new KafkaCursor(timeline.getTopic(), partition, firstOffset);
    }

    @Override
    public NakadiCursor getFirst() {
        // TODO: Support several timelines
        return first.toNakadiCursor(getTimeline());
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
        final KafkaPartitionStartStatistics that = (KafkaPartitionStartStatistics) o;
        return first.equals(that.first) && getTimeline().equals(that.getTimeline())
                && getPartition().equals(that.getPartition());
    }

    @Override
    public int hashCode() {
        int result = first.hashCode();
        result = 31 * result + getTimeline().hashCode();
        result = 31 * result + getPartition().hashCode();
        return result;
    }
}
