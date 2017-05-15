package org.zalando.nakadi.domain;

public class PartitionBaseStatisticsImpl implements PartitionBaseStatistics {

    private final Timeline timeline;

    private final String partition;

    public PartitionBaseStatisticsImpl(final Timeline timeline, final String partition) {
        this.timeline = timeline;
        this.partition = partition;
    }

    @Override
    public Timeline getTimeline() {
        return timeline;
    }

    @Override
    public String getTopic() {
        return timeline.getTopic();
    }

    @Override
    public String getPartition() {
        return partition;
    }
}
