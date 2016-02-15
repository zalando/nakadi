package de.zalando.aruha.nakadi.repository.kafka;

import com.google.common.collect.ImmutableList;
import de.zalando.aruha.nakadi.NakadiException;
import de.zalando.aruha.nakadi.domain.Cursor;
import de.zalando.aruha.nakadi.domain.Topic;
import de.zalando.aruha.nakadi.domain.TopicPartition;
import de.zalando.aruha.nakadi.repository.EventConsumer;
import de.zalando.aruha.nakadi.repository.TopicCreationException;
import de.zalando.aruha.nakadi.repository.TopicRepository;
import de.zalando.aruha.nakadi.repository.zookeeper.ZooKeeperHolder;
import kafka.admin.AdminUtils;
import kafka.utils.ZkUtils;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

import static de.zalando.aruha.nakadi.repository.kafka.KafkaCursor.fromNakadiCursor;
import static de.zalando.aruha.nakadi.repository.kafka.KafkaCursor.toKafkaOffset;
import static de.zalando.aruha.nakadi.repository.kafka.KafkaCursor.toKafkaPartition;
import static de.zalando.aruha.nakadi.repository.kafka.KafkaCursor.toNakadiOffset;
import static de.zalando.aruha.nakadi.repository.kafka.KafkaCursor.toNakadiPartition;

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
            throw new NakadiException("Failed to list topics", e);
        }
    }

    @Override
    public void createTopic(final String topic) throws TopicCreationException {
        createTopic(topic,
                settings.getDefaultTopicPartitionNum(),
                settings.getDefaultTopicReplicaFactor(),
                settings.getDefaultTopicRetentionMs(),
                settings.getDefaultTopicRotationMs());
    }

    @Override
    public void createTopic(final String topic, final int partitionsNum, final int replicaFactor,
                            final long retentionMs, final long rotationMs) throws TopicCreationException {
        ZkUtils zkUtils = null;
        try {
            final String connectionString = zkFactory.get().getZookeeperClient().getCurrentConnectionString();
            zkUtils = ZkUtils.apply(connectionString, settings.getZkSessionTimeoutMs(),
                    settings.getZkConnectionTimeoutMs(), false);

            final Properties topicConfig = new Properties();
            topicConfig.setProperty("retention.ms", Long.toString(retentionMs));
            topicConfig.setProperty("segment.ms", Long.toString(rotationMs));

            AdminUtils.createTopic(zkUtils, topic, partitionsNum, replicaFactor, topicConfig);
        } catch (Exception e) {
            throw new TopicCreationException("unable to create topic", e);
        }
        finally {
            if (zkUtils != null) {
                zkUtils.close();
            }
        }
    }

    @Override
    public boolean topicExists(final String topic) throws NakadiException {
        return listTopics()
                .stream()
                .map(Topic::getName)
                .anyMatch(t -> t.equals(topic));
    }

    @Override
    public boolean partitionExists(final String topic, final String partition) throws NakadiException {
        return kafkaFactory
                .getConsumer()
                .partitionsFor(topic)
                .stream()
                .anyMatch(pInfo -> toNakadiPartition(pInfo.partition()).equals(partition));
    }

    @Override
    public boolean areCursorsValid(final String topic, final List<Cursor> cursors) throws NakadiException {
        final List<TopicPartition> partitions = listPartitions(topic);
        return cursors
                .stream()
                .allMatch(cursor -> partitions
                        .stream()
                        .filter(tp -> tp.getPartitionId().equals(cursor.getPartition()))
                        .findFirst()
                        .map(pInfo -> {
                            final long newestOffset = toKafkaOffset(pInfo.getNewestAvailableOffset());
                            final long oldestOffset = toKafkaOffset(pInfo.getOldestAvailableOffset());
                            try {
                                final long offset = fromNakadiCursor(cursor).getOffset();
                                return offset >= oldestOffset && offset <= newestOffset;
                            } catch (NumberFormatException e) {
                                return false;
                            }
                        })
                        .orElse(false));
    }

    @Override
    public void postEvent(final String topicId, final String partitionId, final String payload) throws NakadiException {
        LOG.info("Posting {} {} {}", topicId, partitionId, payload);

        final ProducerRecord<String, String> record = new ProducerRecord<>(topicId, toKafkaPartition(partitionId),
                partitionId, payload);
        try {
            kafkaProducer.send(record).get(settings.getKafkaSendTimeoutMs(), TimeUnit.MILLISECONDS);
        } catch (InterruptedException | ExecutionException | TimeoutException e) {
            throw new NakadiException("Failed to send event", e);
        }
    }

    @Override
    public List<TopicPartition> listPartitions(final String topicId) throws NakadiException {

        try (final Consumer<String, String> consumer = kafkaFactory.getConsumer()) {

            final List<org.apache.kafka.common.TopicPartition> kafkaTPs = consumer
                    .partitionsFor(topicId)
                    .stream()
                    .map(p -> new org.apache.kafka.common.TopicPartition(topicId, p.partition()))
                    .collect(Collectors.toList());

            consumer.assign(kafkaTPs);

            final org.apache.kafka.common.TopicPartition[] tpArray =
                    kafkaTPs.toArray(new org.apache.kafka.common.TopicPartition[kafkaTPs.size()]);

            consumer.seekToBeginning(tpArray);
            final Map<Integer, Long> earliestOffsets = getPositions(consumer, kafkaTPs);

            consumer.seekToEnd(tpArray);
            final Map<Integer, Long> latestOffsets = getPositions(consumer, kafkaTPs);

            return kafkaTPs
                    .stream()
                    .map(tp -> {
                        final int partition = tp.partition();
                        final TopicPartition topicPartition = new TopicPartition(topicId, toNakadiPartition(partition));
                        topicPartition.setNewestAvailableOffset(toNakadiOffset(latestOffsets.get(partition)));
                        topicPartition.setOldestAvailableOffset(toNakadiOffset(earliestOffsets.get(partition)));
                        return topicPartition;
                    })
                    .collect(Collectors.toList());
        }
        catch (Exception e) {
            throw new NakadiException("Error occurred when fetching partition offsets", e);
        }
    }

    private Map<Integer, Long> getPositions(final Consumer<String, String> consumer,
                                            final List<org.apache.kafka.common.TopicPartition> kafkaTPs) {
        return kafkaTPs
                .stream()
                .collect(Collectors.toMap(
                        org.apache.kafka.common.TopicPartition::partition,
                        consumer::position
                ));
    }

    @Override
    public TopicPartition getPartition(final String topicId, final String partition) throws NakadiException {

        try (final Consumer<String, String> consumer = kafkaFactory.getConsumer()) {

            final org.apache.kafka.common.TopicPartition tp =
                    new org.apache.kafka.common.TopicPartition(topicId, toKafkaPartition(partition));

            consumer.assign(ImmutableList.of(tp));

            final TopicPartition topicPartition = new TopicPartition(topicId, partition);

            consumer.seekToBeginning(tp);
            topicPartition.setOldestAvailableOffset(toNakadiOffset(consumer.position(tp)));

            consumer.seekToEnd(tp);
            topicPartition.setNewestAvailableOffset(toNakadiOffset(consumer.position(tp)));

            return topicPartition;
        }
        catch (Exception e) {
            throw new NakadiException("Error occurred when fetching partition offsets", e);
        }
    }

    @Override
    public EventConsumer createEventConsumer(final String topic, final Map<String, String> cursors) {
        return new NakadiKafkaConsumer(kafkaFactory, topic, cursors, settings.getKafkaPollTimeoutMs());
    }
}
