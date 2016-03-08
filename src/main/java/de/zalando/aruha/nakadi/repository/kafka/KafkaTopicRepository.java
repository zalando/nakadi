package de.zalando.aruha.nakadi.repository.kafka;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import de.zalando.aruha.nakadi.domain.Cursor;
import de.zalando.aruha.nakadi.domain.Topic;
import de.zalando.aruha.nakadi.domain.TopicPartition;
import de.zalando.aruha.nakadi.exceptions.DuplicatedEventTypeNameException;
import de.zalando.aruha.nakadi.exceptions.NakadiException;
import de.zalando.aruha.nakadi.exceptions.ServiceUnavailableException;
import de.zalando.aruha.nakadi.exceptions.TopicDeletionException;
import de.zalando.aruha.nakadi.repository.EventConsumer;
import de.zalando.aruha.nakadi.exceptions.TopicCreationException;
import de.zalando.aruha.nakadi.repository.TopicRepository;
import de.zalando.aruha.nakadi.repository.zookeeper.ZooKeeperHolder;
import kafka.admin.AdminUtils;
import kafka.common.TopicExistsException;
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
import static de.zalando.aruha.nakadi.repository.kafka.KafkaCursor.kafkaCursor;
import static de.zalando.aruha.nakadi.repository.kafka.KafkaCursor.toKafkaOffset;
import static de.zalando.aruha.nakadi.repository.kafka.KafkaCursor.toKafkaPartition;
import static de.zalando.aruha.nakadi.repository.kafka.KafkaCursor.toNakadiOffset;
import static de.zalando.aruha.nakadi.repository.kafka.KafkaCursor.toNakadiPartition;
import static java.util.Collections.unmodifiableList;
import static java.util.stream.Collectors.toList;

public class KafkaTopicRepository implements TopicRepository {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaTopicRepository.class);

    private final ZooKeeperHolder zkFactory;
    private final Producer<String, String> kafkaProducer;
    private final KafkaFactory kafkaFactory;
    private final KafkaRepositorySettings settings;

    public KafkaTopicRepository(final ZooKeeperHolder zkFactory, final KafkaFactory kafkaFactory,
                                final KafkaRepositorySettings settings) {
        this.zkFactory = zkFactory;
        this.kafkaProducer = kafkaFactory.createProducer();
        this.kafkaFactory = kafkaFactory;
        this.settings = settings;
    }

    @Override
    public List<Topic> listTopics() throws ServiceUnavailableException {
        try {
            return zkFactory.get()
                    .getChildren()
                    .forPath("/brokers/topics")
                    .stream()
                    .map(Topic::new)
                    .collect(toList());
        } catch (Exception e) {
            throw new ServiceUnavailableException("Failed to list topics", e);
        }
    }

    @Override
    public void createTopic(final String topic) throws TopicCreationException, DuplicatedEventTypeNameException {
        createTopic(topic,
                settings.getDefaultTopicPartitionNum(),
                settings.getDefaultTopicReplicaFactor(),
                settings.getDefaultTopicRetentionMs(),
                settings.getDefaultTopicRotationMs());
    }

    @Override
    public void createTopic(final String topic, final int partitionsNum, final int replicaFactor,
                            final long retentionMs, final long rotationMs)
            throws TopicCreationException, DuplicatedEventTypeNameException {
        try {
            doWithZkUtils(zkUtils -> {
                final Properties topicConfig = new Properties();
                topicConfig.setProperty("retention.ms", Long.toString(retentionMs));
                topicConfig.setProperty("segment.ms", Long.toString(rotationMs));
                AdminUtils.createTopic(zkUtils, topic, partitionsNum, replicaFactor, topicConfig);
            });
        }
        catch (TopicExistsException e) {
            throw new DuplicatedEventTypeNameException("EventType with name " + topic +
                    " already exists (or wasn't completely removed yet)");
        }
        catch (Exception e) {
            throw new TopicCreationException("Unable to create topic " + topic, e);
        }
    }

    @Override
    public void deleteTopic(final String topic) throws TopicDeletionException {
        try {
            // this will only trigger topic deletion, but the actual deletion is asynchronous
            doWithZkUtils(zkUtils -> AdminUtils.deleteTopic(zkUtils, topic));
        }
        catch (Exception e) {
            throw new TopicDeletionException("Unable to delete topic " + topic, e);
        }
    }

    @Override
    public boolean topicExists(final String topic) throws ServiceUnavailableException {
        return listTopics()
                .stream()
                .map(Topic::getName)
                .anyMatch(t -> t.equals(topic));
    }

    @Override
    public boolean partitionExists(final String topic, final String partition) throws NakadiException {
        return listPartitionNames(topic).stream()
                .anyMatch(partition::equals);
    }

    @Override
    public boolean areCursorsValid(final String topic, final List<Cursor> cursors) throws ServiceUnavailableException {
        final List<TopicPartition> partitions = listPartitions(topic);
        return cursors
                .stream()
                .allMatch(cursor -> partitions
                        .stream()
                        .filter(tp -> tp.getPartitionId().equals(cursor.getPartition()))
                        .findFirst()
                        .map(pInfo -> {
                            if (Cursor.BEFORE_OLDEST_OFFSET.equals(cursor.getOffset())) {
                                return true;
                            }
                            else if (Cursor.BEFORE_OLDEST_OFFSET.equals(pInfo.getNewestAvailableOffset())) {
                                return false;
                            }
                            final long newestOffset = toKafkaOffset(pInfo.getNewestAvailableOffset());
                            final long oldestOffset = toKafkaOffset(pInfo.getOldestAvailableOffset());
                            try {
                                final long offset = fromNakadiCursor(cursor).getOffset();
                                return offset >= oldestOffset - 1 && offset <= newestOffset;
                            } catch (NumberFormatException e) {
                                return false;
                            }
                        })
                        .orElse(false));
    }

    @Override
    public void postEvent(final String topicId, final String partitionId, final String payload) throws ServiceUnavailableException {
        LOG.info("Posting {} {} {}", topicId, partitionId, payload);

        final ProducerRecord<String, String> record = new ProducerRecord<>(topicId, toKafkaPartition(partitionId),
                partitionId, payload);
        try {
            kafkaProducer.send(record).get(settings.getKafkaSendTimeoutMs(), TimeUnit.MILLISECONDS);
        } catch (InterruptedException | ExecutionException | TimeoutException e) {
            throw new ServiceUnavailableException("Failed to send event", e);
        }
    }

    @Override
    public List<TopicPartition> listPartitions(final String topicId) throws ServiceUnavailableException {

        try (final Consumer<String, String> consumer = kafkaFactory.getConsumer()) {

            final List<org.apache.kafka.common.TopicPartition> kafkaTPs = consumer
                    .partitionsFor(topicId)
                    .stream()
                    .map(p -> new org.apache.kafka.common.TopicPartition(topicId, p.partition()))
                    .collect(toList());

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

                        final Long latestOffset = latestOffsets.get(partition);
                        topicPartition.setNewestAvailableOffset(transformNewestOffset(latestOffset));

                        topicPartition.setOldestAvailableOffset(toNakadiOffset(earliestOffsets.get(partition)));
                        return topicPartition;
                    })
                    .collect(toList());
        }
        catch (Exception e) {
            throw new ServiceUnavailableException("Error occurred when fetching partitions offsets", e);
        }
    }

    @Override
    public List<String> listPartitionNames(final String topicId) throws NakadiException {
        return unmodifiableList(kafkaFactory.createProducer().partitionsFor(topicId)
                .stream()
                .map(partitionInfo -> String.valueOf(partitionInfo.partition()))
                .collect(toList()));
    }

    private String transformNewestOffset(final Long newestOffset) {
        return newestOffset == 0 ? Cursor.BEFORE_OLDEST_OFFSET : toNakadiOffset(newestOffset - 1);
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
    public TopicPartition getPartition(final String topicId, final String partition) throws ServiceUnavailableException {
        try (final Consumer<String, String> consumer = kafkaFactory.getConsumer()) {

            final org.apache.kafka.common.TopicPartition tp =
                    new org.apache.kafka.common.TopicPartition(topicId, toKafkaPartition(partition));

            consumer.assign(ImmutableList.of(tp));

            final TopicPartition topicPartition = new TopicPartition(topicId, partition);

            consumer.seekToBeginning(tp);
            topicPartition.setOldestAvailableOffset(toNakadiOffset(consumer.position(tp)));

            consumer.seekToEnd(tp);
            final Long latestOffset = consumer.position(tp);
            topicPartition.setNewestAvailableOffset(transformNewestOffset(latestOffset));

            return topicPartition;
        }
        catch (Exception e) {
            throw new ServiceUnavailableException("Error occurred when fetching partition offsets", e);
        }
    }

    @Override
    public EventConsumer createEventConsumer(final String topic, final Map<String, String> cursors)
            throws ServiceUnavailableException {

        final List<KafkaCursor> kafkaCursors = Lists.newArrayListWithCapacity(cursors.size());

        for (final Map.Entry<String, String> entry : cursors.entrySet()) {
            final String offset = entry.getValue();
            final String partition = entry.getKey();

            final long kafkaOffset;
            if (Cursor.BEFORE_OLDEST_OFFSET.equals(offset)) {
                final TopicPartition tp = getPartition(topic, partition);
                kafkaOffset = toKafkaOffset(tp.getOldestAvailableOffset());

            }
            else {
                kafkaOffset = toKafkaOffset(offset) + 1L;
            }

            final KafkaCursor kafkaCursor = kafkaCursor(toKafkaPartition(partition), kafkaOffset);
            kafkaCursors.add(kafkaCursor);
        }

        return kafkaFactory.createNakadiConsumer(topic, kafkaCursors, settings.getKafkaPollTimeoutMs());
    }

    @FunctionalInterface
    private interface ZkUtilsAction {
        void execute(ZkUtils zkUtils) throws Exception;
    }

    private void doWithZkUtils(final ZkUtilsAction action) throws Exception {
        ZkUtils zkUtils = null;
        try {
            final String connectionString = zkFactory.get().getZookeeperClient().getCurrentConnectionString();
            zkUtils = ZkUtils.apply(connectionString, settings.getZkSessionTimeoutMs(),
                    settings.getZkConnectionTimeoutMs(), false);
            action.execute(zkUtils);
        }
        finally {
            if (zkUtils != null) {
                zkUtils.close();
            }
        }
    }
}
