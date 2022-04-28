package org.zalando.nakadi.domain;

public interface NakadiMetadata {

    String getEid();

    String getPartitionStr();

    Integer getPartitionInt();

    String getEventType();

}
