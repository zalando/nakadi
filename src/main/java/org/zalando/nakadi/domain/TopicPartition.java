package org.zalando.nakadi.domain;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
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
}
