package org.zalando.nakadi.domain;

public interface NakadiMetadata {

    String getEid();

    void setPartition(String partition);

    String getPartitionStr();

    Integer getPartitionInt();

    String getOccurredAt();

    String getEventType();

    void setEventType(String eventType);

    String getPublishedBy();

    void setPublishedBy(String publisher);

    String getReceivedAt();

    void setReceivedAt(String toString);

    String getFlowId();

    void setFlowId(String flowId);

    String getSchemaVersion();

    void setSchemaVersion(String toString);
}
