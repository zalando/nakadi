package org.zalando.nakadi.domain;

public interface PartitionStartStatistics extends PartitionBaseStatistics {

    NakadiCursor getFirst();

    NakadiCursor getBeforeFirst();
}
