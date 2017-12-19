package org.zalando.nakadi.repository;

import org.zalando.nakadi.domain.BatchItem;
import org.zalando.nakadi.domain.NakadiCursor;
import org.zalando.nakadi.domain.PartitionEndStatistics;
import org.zalando.nakadi.domain.PartitionStatistics;
import org.zalando.nakadi.domain.Timeline;
import org.zalando.nakadi.exceptions.EventPublishingException;
import org.zalando.nakadi.exceptions.InvalidCursorException;
import org.zalando.nakadi.exceptions.NakadiException;
import org.zalando.nakadi.exceptions.ServiceUnavailableException;
import org.zalando.nakadi.exceptions.TopicCreationException;
import org.zalando.nakadi.exceptions.TopicDeletionException;
import org.zalando.nakadi.exceptions.runtime.TopicConfigException;
import org.zalando.nakadi.exceptions.runtime.TopicRepositoryException;

import java.util.Collection;
import java.util.List;
import java.util.Optional;

public interface TopicRepository {

    class TimelinePartition {
        private final Timeline timeline;
        private final String partition;

        public TimelinePartition(final Timeline timeline, final String partition) {
            this.timeline = timeline;
            this.partition = partition;
        }

        public Timeline getTimeline() {
            return timeline;
        }

        public String getPartition() {
            return partition;
        }
    }

    String createTopic(int partitionCount, Long retentionTimeMs) throws TopicCreationException;

    void deleteTopic(String topic) throws TopicDeletionException;

    boolean topicExists(String topic) throws TopicRepositoryException;

    void syncPostBatch(String topicId, List<BatchItem> batch) throws EventPublishingException;

    Optional<PartitionStatistics> loadPartitionStatistics(Timeline timeline, String partition)
            throws ServiceUnavailableException;

    /**
     * Returns partitions statistics about requested partitions. The order and the amount of response items is exactly
     * the same as it was requested
     *
     * @param partitions Partitions to query data for
     * @return List of statistics.
     * @throws ServiceUnavailableException In case when there was a problem communicating with storage
     */
    List<Optional<PartitionStatistics>> loadPartitionStatistics(Collection<TimelinePartition> partitions)
            throws ServiceUnavailableException;

    List<PartitionStatistics> loadTopicStatistics(Collection<Timeline> timelines) throws ServiceUnavailableException;

    List<PartitionEndStatistics> loadTopicEndStatistics(Collection<Timeline> topics) throws ServiceUnavailableException;

    List<String> listPartitionNames(String topicId);

    EventConsumer.LowLevelConsumer createEventConsumer(String clientId, List<NakadiCursor> positions)
            throws NakadiException, InvalidCursorException;

    void validateReadCursors(List<NakadiCursor> cursors) throws InvalidCursorException,
            ServiceUnavailableException;

    void setRetentionTime(String topic, Long retentionMs) throws TopicConfigException;
}
