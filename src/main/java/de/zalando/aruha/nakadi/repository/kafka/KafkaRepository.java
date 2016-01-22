package de.zalando.aruha.nakadi.repository.kafka;

import de.zalando.aruha.nakadi.NakadiException;
import de.zalando.aruha.nakadi.domain.Cursor;
import de.zalando.aruha.nakadi.domain.Topic;
import de.zalando.aruha.nakadi.domain.TopicPartitionOffsets;
import de.zalando.aruha.nakadi.repository.EventConsumer;
import de.zalando.aruha.nakadi.repository.TopicRepository;
import de.zalando.aruha.nakadi.repository.zookeeper.ZooKeeperHolder;
import kafka.api.PartitionOffsetRequestInfo;
import kafka.common.TopicAndPartition;
import kafka.javaapi.OffsetRequest;
import kafka.javaapi.OffsetResponse;
import kafka.javaapi.consumer.SimpleConsumer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;
import java.util.stream.Collectors;

public class KafkaRepository implements TopicRepository {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaRepository.class);

    private static final long KAFKA_SEND_TIMEOUT_MS = 10000;

    private final ZooKeeperHolder zkFactory;
    private final Producer<String, String> kafkaProducer;
    private final KafkaFactory kafkaFactory;
    private final long kafkaPollTimeout;

    public KafkaRepository(final ZooKeeperHolder zkFactory, final KafkaFactory kafkaFactory,
                           final long kafkaPollTimeout) {
        this.zkFactory = zkFactory;
        this.kafkaProducer = kafkaFactory.createProducer();
        this.kafkaFactory = kafkaFactory;
        this.kafkaPollTimeout = kafkaPollTimeout;
    }

    @Override
    public List<Topic> listTopics() throws NakadiException {
        try {
            return zkFactory.get().getChildren().forPath("/brokers/topics").stream().map(Topic::new).collect(Collectors.toList());
        } catch (Exception e) {
            throw new NakadiException("Failed to get partitions", e);
        }
    }

    @Override
    public void postEvent(final String topicId, final String partitionId, final String payload) throws NakadiException {
        LOG.info("Posting {} {} {}", topicId, partitionId, payload);

        final ProducerRecord<String, String> record = new ProducerRecord<>(topicId, Integer.parseInt(partitionId),
                partitionId, payload);
        try {
            kafkaProducer.send(record).get(KAFKA_SEND_TIMEOUT_MS, TimeUnit.MILLISECONDS);
        } catch (InterruptedException | ExecutionException | TimeoutException e) {
            throw new NakadiException("Failed to send event", e);
        }
    }

    public List<String> listPartitions(final String topic) {
        return kafkaProducer
                .partitionsFor(topic)
                .stream()
                .map(partitionInfo -> Integer.toString(partitionInfo.partition()))
                .collect(Collectors.toList());
    }

    @Override
    public List<TopicPartitionOffsets> listPartitionsOffsets(final String topicId) {

        final SimpleConsumer sc = kafkaFactory.getSimpleConsumer();
        try {
            final List<TopicAndPartition> partitions = kafkaFactory.getConsumer().partitionsFor(topicId).stream().map(p ->
                        new TopicAndPartition(p.topic(), p.partition())).collect(Collectors.toList());

            final Map<TopicAndPartition, PartitionOffsetRequestInfo> latestPartitionRequests = partitions.stream()
                                                                                                         .collect(
                                                                                                             Collectors
                                                                                                                     .toMap(
                                                                                                                         Function
                                                                                                                             .identity(),
                                                                                                                         t ->
                                                                                                                             new PartitionOffsetRequestInfo(
                                                                                                                                 kafka
                                                                                                                                     .api
                                                                                                                                     .OffsetRequest
                                                                                                                                     .LatestTime(),
                                                                                                                                 1)));
                final Map<TopicAndPartition, PartitionOffsetRequestInfo> earliestPartitionRequests = partitions.stream()
                                                                                                               .collect(
                                                                                                                   Collectors
                                                                                                                           .toMap(
                                                                                                                               Function
                                                                                                                                   .identity(),
                                                                                                                               t ->
                                                                                                                                   new PartitionOffsetRequestInfo(
                                                                                                                                       kafka
                                                                                                                                           .api
                                                                                                                                           .OffsetRequest
                                                                                                                                           .EarliestTime(),
                                                                                                                                       1)));

                    final OffsetResponse latestPartitionData = fetchPartitionData(sc, latestPartitionRequests);
                    final OffsetResponse earliestPartitionData = fetchPartitionData(sc, earliestPartitionRequests);

                    return partitions.stream().map(r ->
                                             processTopicPartitionMetadata(r, latestPartitionData,
                                                 earliestPartitionData)).collect(Collectors.toList());
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

            private TopicPartitionOffsets processTopicPartitionMetadata(final TopicAndPartition partition,
                    final OffsetResponse latestPartitionData, final OffsetResponse earliestPartitionData) {

                final TopicPartitionOffsets tp = new TopicPartitionOffsets(partition.topic(),
                        Integer.toString(partition.partition()));
                final long latestOffset = latestPartitionData.offsets(partition.topic(), partition.partition())[0];
                final long earliestOffset = earliestPartitionData.offsets(partition.topic(), partition.partition())[0];

                tp.setNewestAvailableOffset(Long.toString(latestOffset));
                tp.setOldestAvailableOffset(Long.toString(earliestOffset));

                return tp;
            }

            private OffsetResponse fetchPartitionData(final SimpleConsumer sc,
                    final Map<TopicAndPartition, PartitionOffsetRequestInfo> partitionRequests) {
                final OffsetRequest request = new OffsetRequest(partitionRequests,
                        kafka.api.OffsetRequest.CurrentVersion(), "offsetlookup_" + UUID.randomUUID());
                return sc.getOffsetsBefore(request);
            }


    @Override
    public EventConsumer createEventConsumer(final List<Cursor> cursors) {
        return new NakadiKafkaConsumer(kafkaFactory, cursors, kafkaPollTimeout);
    }
}
