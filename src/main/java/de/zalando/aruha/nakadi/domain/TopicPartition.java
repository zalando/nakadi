package de.zalando.aruha.nakadi.domain;

public class TopicPartition {
	private String topicId;
	private String partitionId;

	public TopicPartition() {

	}
	public TopicPartition(String topicId, String partitionId) {
		topicId = topicId;
		partitionId = partitionId;
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
}
