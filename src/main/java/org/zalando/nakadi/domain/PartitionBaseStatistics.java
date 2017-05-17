package org.zalando.nakadi.domain;

public interface PartitionBaseStatistics {

    Timeline getTimeline();

    String getTopic();

    String getPartition();

}
