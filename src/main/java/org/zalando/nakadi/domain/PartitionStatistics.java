package org.zalando.nakadi.domain;

public abstract class PartitionStatistics {
    private final Timeline timeline;
    private final String partition;

    public PartitionStatistics(final Timeline timeline, final String partition) {
        this.timeline = timeline;
        this.partition = partition;
    }

    public Timeline getTimeline() {
        return timeline;
    }

    public String getTopic() {
        return getTimeline().getTopic();
    }

    public String getPartition() {
        return partition;
    }

    public abstract NakadiCursor getFirst();

    public abstract NakadiCursor getLast();

    public abstract NakadiCursor getBeforeFirst();
}
