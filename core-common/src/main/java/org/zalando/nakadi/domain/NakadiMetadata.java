package org.zalando.nakadi.domain;

public interface NakadiMetadata {

    String getEid();

    String getPartition();

    String getEventType();

}
