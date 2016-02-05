package de.zalando.aruha.nakadi.domain;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

public class TopicPartition {

    @JsonIgnore
    private String topicId;

    @JsonProperty("partition")
    private String partitionId;

    private String oldestAvailableOffset;

    private String newestAvailableOffset;

    public TopicPartition(final String topicId, final String partitionId) {
        setTopicId(topicId);
        setPartitionId(partitionId);
    }

    public String getTopicId() {
        return topicId;
    }

    public void setTopicId(final String topicId) {
        this.topicId = topicId;
    }

    public String getPartitionId() {
        return partitionId;
    }

    public void setPartitionId(final String partitionId) {
        this.partitionId = partitionId;
    }

    public String getOldestAvailableOffset() {
        return oldestAvailableOffset;
    }

    public void setOldestAvailableOffset(final String oldestAvailableOffset) {
        this.oldestAvailableOffset = oldestAvailableOffset;
    }

    public String getNewestAvailableOffset() {
        return newestAvailableOffset;
    }

    public void setNewestAvailableOffset(final String newestAvailableOffset) {
        this.newestAvailableOffset = newestAvailableOffset;
    }
}
