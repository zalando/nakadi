package org.zalando.nakadi.domain;

public abstract class PartitionStatistics extends PartitionEndStatistics {

    public PartitionStatistics(final Timeline timeline, final String partition) {
        super(timeline, partition);
    }

    public abstract NakadiCursor getFirst();

    public abstract NakadiCursor getBeforeFirst();
}
