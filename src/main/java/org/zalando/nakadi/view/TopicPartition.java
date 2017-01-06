package org.zalando.nakadi.view;

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

    public TopicPartition(final String topicId, final String partitionId, final String oldestAvailableOffset,
                          final String newestAvailableOffset) {
        this.topicId = topicId;
        this.partitionId = partitionId;
        this.oldestAvailableOffset = oldestAvailableOffset;
        this.newestAvailableOffset = newestAvailableOffset;
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

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }

        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        final TopicPartition that = (TopicPartition) o;

        if (topicId != null ? !topicId.equals(that.topicId) : that.topicId != null) {
            return false;
        }

        if (partitionId != null ? !partitionId.equals(that.partitionId) : that.partitionId != null) {
            return false;
        }

        if (oldestAvailableOffset != null ? !oldestAvailableOffset.equals(that.oldestAvailableOffset)
                                          : that.oldestAvailableOffset != null) {
            return false;
        }

        return newestAvailableOffset != null ? newestAvailableOffset.equals(that.newestAvailableOffset)
                                             : that.newestAvailableOffset == null;

    }

    @Override
    public int hashCode() {
        int result = topicId != null ? topicId.hashCode() : 0;
        result = 31 * result + (partitionId != null ? partitionId.hashCode() : 0);
        result = 31 * result + (oldestAvailableOffset != null ? oldestAvailableOffset.hashCode() : 0);
        result = 31 * result + (newestAvailableOffset != null ? newestAvailableOffset.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "TopicPartition{" +
                "topicId='" + topicId + '\'' +
                ", partitionId='" + partitionId + '\'' +
                ", oldestAvailableOffset='" + oldestAvailableOffset + '\'' +
                ", newestAvailableOffset='" + newestAvailableOffset + '\'' +
                '}';
    }
}
