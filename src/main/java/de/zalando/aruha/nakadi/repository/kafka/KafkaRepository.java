package de.zalando.aruha.nakadi.repository.kafka;

import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;
import java.util.stream.Collectors;

import de.zalando.aruha.nakadi.domain.Cursor;
import kafka.admin.AdminUtils;
import kafka.utils.ZkUtils;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.springframework.beans.factory.annotation.Autowired;

import org.springframework.stereotype.Component;

import de.zalando.aruha.nakadi.NakadiException;
import de.zalando.aruha.nakadi.domain.Topic;
import de.zalando.aruha.nakadi.domain.TopicPartition;
import de.zalando.aruha.nakadi.repository.EventConsumer;
import de.zalando.aruha.nakadi.repository.TopicRepository;
import de.zalando.aruha.nakadi.repository.zookeeper.ZooKeeperHolder;

import kafka.api.PartitionOffsetRequestInfo;

import kafka.common.TopicAndPartition;

import kafka.javaapi.OffsetRequest;
import kafka.javaapi.OffsetResponse;

import kafka.javaapi.consumer.SimpleConsumer;

public class KafkaRepository implements TopicRepository {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaRepository.class);

    private final ZooKeeperHolder zkFactory;
    private final Producer<String, String> kafkaProducer;
    private final KafkaFactory kafkaFactory;
    private final KafkaRepositorySettings settings;

    public KafkaRepository(final ZooKeeperHolder zkFactory, final KafkaFactory kafkaFactory,
                           final KafkaRepositorySettings settings) {
        this.zkFactory = zkFactory;
        this.kafkaProducer = kafkaFactory.createProducer();
        this.kafkaFactory = kafkaFactory;
        this.settings = settings;
    }

    @Override
    public List<Topic> listTopics() throws NakadiException {
        try {
            return zkFactory.get()
                    .getChildren()
                    .forPath("/brokers/topics")
                    .stream()
                    .map(Topic::new)
                    .collect(Collectors.toList());
        } catch (Exception e) {
            throw new NakadiException("Failed to get partitions", e);
        }
    }

    @Override
    public void createTopic(final String topic) {
        createTopic(topic,
                settings.getDefaultTopicPartitionNum(),
                settings.getDefaultTopicReplicaFactor(),
                settings.getDefaultTopicRetentionMs(),
                settings.getDefaultTopicRotationMs());
    }

    @Override
    public void createTopic(final String topic, final int partitionsNum, final int replicaFactor,
                            final long retentionMs, final long rotationMs) {

        final String connectionString = zkFactory.get().getZookeeperClient().getCurrentConnectionString();
        final ZkUtils zkUtils = ZkUtils.apply(connectionString, settings.getZkSessionTimeoutMs(),
                settings.getZkConnectionTimeoutMs(), false);
        try {
            final Properties topicConfig = new Properties();
            topicConfig.setProperty("retention.ms", Long.toString(retentionMs));
            topicConfig.setProperty("segment.ms", Long.toString(rotationMs));

            AdminUtils.createTopic(zkUtils, topic, partitionsNum, replicaFactor, topicConfig);
        }
        finally {
            zkUtils.close();
        }
    }

    public boolean topicExists(final String topic) throws NakadiException {
        return listTopics()
                .stream()
                .map(Topic::getName)
                .anyMatch(t -> t.equals(topic));
    }

    public boolean areCursorsCorrect(final String topic, final List<Cursor> cursors) {
        final List<TopicPartition> partitions = listPartitions(topic);
        return cursors
                .stream()
                .allMatch(cursor -> partitions
                        .stream()
                        .filter(tp -> tp.getPartitionId().equals(cursor.getPartition()))
                        .findFirst()
                        .map(pInfo -> {
                            final long newestOffset = Long.parseLong(pInfo.getNewestAvailableOffset());
                            final long oldestOffset = Long.parseLong(pInfo.getOldestAvailableOffset());
                            final long offset = Long.parseLong(cursor.getOffset());
                            return offset >= oldestOffset && offset <= newestOffset;
                        })
                        .orElse(false));
    }

    @Override
    public void postEvent(final String topicId, final String partitionId, final String payload) throws NakadiException {
        LOG.info("Posting {} {} {}", topicId, partitionId, payload);

        final ProducerRecord<String, String> record = new ProducerRecord<>(topicId, Integer.parseInt(partitionId),
                partitionId, payload);
        try {
            kafkaProducer.send(record).get(settings.getKafkaSendTimeoutMs(), TimeUnit.MILLISECONDS);
        } catch (InterruptedException | ExecutionException | TimeoutException e) {
            throw new NakadiException("Failed to send event", e);
        }
    }

    @Override
    public List<TopicPartition> listPartitions(final String topicId) {

        final SimpleConsumer sc = kafkaFactory.getSimpleConsumer();
        try {
            final List<TopicAndPartition> partitions = kafkaFactory
                    .getConsumer()
                    .partitionsFor(topicId)
                    .stream()
                    .map(p -> new TopicAndPartition(p.topic(), p.partition()))
                    .collect(Collectors.toList());

            final Map<TopicAndPartition, PartitionOffsetRequestInfo> latestPartitionRequests = partitions
                    .stream()
                    .collect(Collectors.toMap(
                            Function.identity(),
                            t -> new PartitionOffsetRequestInfo(kafka.api.OffsetRequest.LatestTime(), 1)));
            final Map<TopicAndPartition, PartitionOffsetRequestInfo> earliestPartitionRequests = partitions
                    .stream()
                    .collect(Collectors.toMap(
                            Function.identity(),
                            t -> new PartitionOffsetRequestInfo(kafka.api.OffsetRequest.EarliestTime(), 1)));

            final OffsetResponse latestPartitionData = fetchPartitionData(sc, latestPartitionRequests);
            final OffsetResponse earliestPartitionData = fetchPartitionData(sc, earliestPartitionRequests);

            return partitions
                    .stream()
                    .map(r -> processTopicPartitionMetadata(r, latestPartitionData, earliestPartitionData))
                    .collect(Collectors.toList());
        } finally {
            sc.close();
        }
    }

    @Override
    public boolean validateOffset(final String offsetToCheck, final String newestOffset,
                                  final String oldestOffset) {
        final long offset = Long.parseLong(offsetToCheck);
        final long newest = Long.parseLong(newestOffset);
        final long oldest = Long.parseLong(oldestOffset);
        return offset >= oldest && offset <= newest;
    }

    private TopicPartition processTopicPartitionMetadata(final TopicAndPartition partition,
                                                         final OffsetResponse latestPartitionData,
                                                         final OffsetResponse earliestPartitionData) {

        final TopicPartition tp = new TopicPartition(partition.topic(),
                Integer.toString(partition.partition()));
        final long latestOffset = latestPartitionData.offsets(partition.topic(), partition.partition())[0];
        final long earliestOffset = earliestPartitionData.offsets(partition.topic(), partition.partition())[0];

        tp.setNewestAvailableOffset(Long.toString(latestOffset));
        tp.setOldestAvailableOffset(Long.toString(earliestOffset));

        return tp;
    }

    private OffsetResponse fetchPartitionData(final SimpleConsumer sc,
                                              final Map<TopicAndPartition,
                                                      PartitionOffsetRequestInfo> partitionRequests) {
        final OffsetRequest request = new OffsetRequest(partitionRequests,
                kafka.api.OffsetRequest.CurrentVersion(), "offsetlookup_" + UUID.randomUUID());
        return sc.getOffsetsBefore(request);
    }

    @Override
    public EventConsumer createEventConsumer(final String topic, final Map<String, String> cursors) {
        return new NakadiKafkaConsumer(kafkaFactory, topic, cursors, settings.getKafkaPollTimeoutMs());
    }
}
