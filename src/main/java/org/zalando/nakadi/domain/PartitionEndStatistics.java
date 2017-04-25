package org.zalando.nakadi.domain;

public interface PartitionEndStatistics extends PartitionBaseStatistics {

    NakadiCursor getLast();

}
