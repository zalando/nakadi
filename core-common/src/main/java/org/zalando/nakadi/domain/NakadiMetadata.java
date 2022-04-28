package org.zalando.nakadi.domain;

import java.util.List;

public interface NakadiMetadata {

    String getEid();

    String getOccurredAt();

    String getEventType();

    void setEventType(String eventType);

    String getPartitionStr();

    Integer getPartitionInt();

    void setPartition(String partition);

    String getPublishedBy();

    void setPublishedBy(String publisher);

    String getReceivedAt();

    void setReceivedAt(String receivedAt);

    String getFlowId();

    void setFlowId(String flowId);

    String getSchemaVersion();

    void setSchemaVersion(String schemaVersion);

    List<String> getPartitionKeys();

    void setPartitionKeys(List<String> partitionKeys);

    List<String> getPartitionCompactionKeys();

    void setPartitionCompactionKeys(List<String> partitionCompactionKeys);
}