package de.zalando.aruha.nakadi.repository.kafka;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import de.zalando.aruha.nakadi.domain.BatchItem;
import de.zalando.aruha.nakadi.domain.Cursor;
import de.zalando.aruha.nakadi.domain.EventPublishingStatus;
import de.zalando.aruha.nakadi.domain.EventPublishingStep;
import de.zalando.aruha.nakadi.domain.EventTypeStatistics;
import de.zalando.aruha.nakadi.domain.Topic;
import de.zalando.aruha.nakadi.domain.TopicPartition;
import de.zalando.aruha.nakadi.exceptions.DuplicatedEventTypeNameException;
import de.zalando.aruha.nakadi.exceptions.EventPublishingException;
import de.zalando.aruha.nakadi.exceptions.InvalidCursorException;
import de.zalando.aruha.nakadi.exceptions.NakadiException;
import de.zalando.aruha.nakadi.exceptions.ServiceUnavailableException;
import de.zalando.aruha.nakadi.exceptions.TopicCreationException;
import de.zalando.aruha.nakadi.exceptions.TopicDeletionException;
import de.zalando.aruha.nakadi.repository.EventConsumer;
import de.zalando.aruha.nakadi.repository.TopicRepository;
import de.zalando.aruha.nakadi.repository.zookeeper.ZooKeeperHolder;
import java.util.stream.Stream;
import kafka.admin.AdminUtils;
import kafka.common.TopicExistsException;
import kafka.utils.ZkUtils;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.producer.BufferExhaustedException;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.InterruptException;
import org.apache.kafka.common.errors.SerializationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static de.zalando.aruha.nakadi.domain.CursorError.EMPTY_PARTITION;
import static de.zalando.aruha.nakadi.domain.CursorError.INVALID_FORMAT;
import static de.zalando.aruha.nakadi.domain.CursorError.NULL_OFFSET;
import static de.zalando.aruha.nakadi.domain.CursorError.NULL_PARTITION;
import static de.zalando.aruha.nakadi.domain.CursorError.PARTITION_NOT_FOUND;
import static de.zalando.aruha.nakadi.domain.CursorError.UNAVAILABLE;
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
    private final KafkaPartitionsCalculator partitionsCalculator;

    public KafkaTopicRepository(final ZooKeeperHolder zkFactory, final KafkaFactory kafkaFactory,
                                final KafkaRepositorySettings settings, final KafkaPartitionsCalculator partitionsCalculator) {
        this.zkFactory = zkFactory;
        this.partitionsCalculator = partitionsCalculator;
        this.kafkaProducer = kafkaFactory.getProducer();
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
        } catch (final Exception e) {
            throw new ServiceUnavailableException("Failed to list topics", e);
        }
    }

    @Override
    public void createTopic(final String topic, final EventTypeStatistics statistics) throws TopicCreationException, DuplicatedEventTypeNameException {
        createTopic(topic,
                calculateKafkaPartitionCount(statistics),
                settings.getDefaultTopicReplicaFactor(),
                settings.getDefaultTopicRetentionMs(),
                settings.getDefaultTopicRotationMs());
    }

    private void createTopic(final String topic, final int partitionsNum, final int replicaFactor,
                             final long retentionMs, final long rotationMs)
            throws TopicCreationException, DuplicatedEventTypeNameException {
        try {
            doWithZkUtils(zkUtils -> {
                final Properties topicConfig = new Properties();
                topicConfig.setProperty("retention.ms", Long.toString(retentionMs));
                topicConfig.setProperty("segment.ms", Long.toString(rotationMs));
                AdminUtils.createTopic(zkUtils, topic, partitionsNum, replicaFactor, topicConfig);
            });
        } catch (final TopicExistsException e) {
            throw new DuplicatedEventTypeNameException("EventType with name " + topic +
                    " already exists (or wasn't completely removed yet)");
        } catch (final Exception e) {
            throw new TopicCreationException("Unable to create topic " + topic, e);
        }
    }

    @Override
    public void deleteTopic(final String topic) throws TopicDeletionException {
        try {
            // this will only trigger topic deletion, but the actual deletion is asynchronous
            doWithZkUtils(zkUtils -> AdminUtils.deleteTopic(zkUtils, topic));
        } catch (final Exception e) {
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
    public void syncPostBatch(final String topicId, final List<BatchItem> batch) throws EventPublishingException {
        final CountDownLatch done = new CountDownLatch(batch.size());

        for (final BatchItem item : batch) {
            final ProducerRecord<String, String> record = new ProducerRecord<>(topicId,
                    toKafkaPartition(item.getPartition()), item.getPartition(), item.getEvent().toString());

            item.setStep(EventPublishingStep.PUBLISHING);

            try {
                kafkaProducer.send(record, kafkaSendCallback(item, done));
            } catch (InterruptException | SerializationException | BufferExhaustedException e) {
                item.updateStatusAndDetail(EventPublishingStatus.FAILED, "internal error");
                throw new EventPublishingException("Error publishing message to kafka", e);
            }
        }

        try {
            final boolean isSuccessful = done.await(settings.getKafkaSendTimeoutMs(), TimeUnit.MILLISECONDS);

            if (!isSuccessful) {
                failBatch(batch, "timed out");
                throw new EventPublishingException("Timeout publishing events");
            }
        } catch (final InterruptedException e) {
            failBatch(batch, "internal error");
            throw new EventPublishingException("Error publishing message to kafka", e);
        }
    }

    private void failBatch(final List<BatchItem> batch, final String reason) {
        for (final BatchItem item : batch) {
            item.updateStatusAndDetail(EventPublishingStatus.FAILED, reason);
        }
    }

    private Callback kafkaSendCallback(final BatchItem item, final CountDownLatch done) {
        return (metadata, exception) -> {
            if (exception == null) {
                item.updateStatusAndDetail(EventPublishingStatus.SUBMITTED, "");
            } else {
                LOG.error("Failed to publish event", exception);
                item.updateStatusAndDetail(EventPublishingStatus.FAILED, "internal error");
            }

            done.countDown();
        };
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
        } catch (final Exception e) {
            throw new ServiceUnavailableException("Error occurred when fetching partitions offsets", e);
        }
    }

    @Override
    public Map<String, Long> materializePositions(final String topicId, String position) throws ServiceUnavailableException {
        try (final Consumer<String, String> consumer = kafkaFactory.getConsumer()) {
            final org.apache.kafka.common.TopicPartition[] kafkaTPs = consumer
                    .partitionsFor(topicId)
                    .stream()
                    .map(p -> new org.apache.kafka.common.TopicPartition(topicId, p.partition()))
                    .toArray(org.apache.kafka.common.TopicPartition[]::new);
            if (position.equals(Cursor.BEFORE_OLDEST_OFFSET)) {
                consumer.seekToBeginning(kafkaTPs);
            } else if (position.equals(Cursor.AFTER_NEWEST_OFFSET)) {
                consumer.seekToEnd(kafkaTPs);
            } else {
                throw new ServiceUnavailableException("Bad offset specification " + position + " for topic " + topicId);
            }
            return Stream.of(kafkaTPs).collect(Collectors.toMap(
                    tp -> String.valueOf(tp.partition()),
                    consumer::position));
        } catch (final Exception e) {
            throw new ServiceUnavailableException("Error occurred when fetching partitions offsets", e);
        }

    }

    @Override
    public List<String> listPartitionNames(final String topicId) {
        return unmodifiableList(kafkaFactory.getProducer().partitionsFor(topicId)
                .stream()
                .map(partitionInfo -> toNakadiPartition(partitionInfo.partition()))
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
        catch (final Exception e) {
            throw new ServiceUnavailableException("Error occurred when fetching partition offsets", e);
        }
    }

    public Consumer<String, String> createKafkaConsumer() {
        return kafkaFactory.getConsumer();
    }

    @Override
    public EventConsumer createEventConsumer(final String topic, final List<Cursor> cursors)
            throws ServiceUnavailableException, InvalidCursorException {
        this.validateCursors(topic, cursors);

        final List<KafkaCursor> kafkaCursors = Lists.newArrayListWithCapacity(cursors.size());

        for (final Cursor cursor : cursors) {
            final String offset = cursor.getOffset();
            final String partition = cursor.getPartition();

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

    private void validateCursors(final String topic, final List<Cursor> cursors) throws ServiceUnavailableException,
            InvalidCursorException {
        final List<TopicPartition> partitions = listPartitions(topic);

        for (final Cursor cursor : cursors) {
            if (cursor.getPartition() == null) {
                throw  new InvalidCursorException(NULL_PARTITION, cursor);
            }

            if (cursor.getOffset() == null) {
                throw new InvalidCursorException(NULL_OFFSET, cursor);
            }

            final TopicPartition topicPartition = partitions
                        .stream()
                        .filter(tp -> tp.getPartitionId().equals(cursor.getPartition()))
                        .findFirst()
                        .orElseThrow(() -> new InvalidCursorException(PARTITION_NOT_FOUND, cursor));

            if (Cursor.BEFORE_OLDEST_OFFSET.equals(cursor.getOffset())) {
                continue;
            } else if (Cursor.BEFORE_OLDEST_OFFSET.equals(topicPartition.getNewestAvailableOffset())) {
                throw new InvalidCursorException(EMPTY_PARTITION, cursor);
            }

            final long newestOffset = toKafkaOffset(topicPartition.getNewestAvailableOffset());
            final long oldestOffset = toKafkaOffset(topicPartition.getOldestAvailableOffset());
            try {
                final long offset = fromNakadiCursor(cursor).getOffset();
                if (offset < oldestOffset - 1 || offset > newestOffset) {
                    throw new InvalidCursorException(UNAVAILABLE, cursor);
                }
            } catch (final NumberFormatException e) {
                throw new InvalidCursorException(INVALID_FORMAT, cursor);
            }
        }
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

    @VisibleForTesting
    Integer calculateKafkaPartitionCount(final EventTypeStatistics stat) {
        if (null == stat) {
            return settings.getDefaultTopicPartitionCount();
        }
        final int maxPartitionsDueParallelism = Math.max(stat.getReadParallelism(), stat.getWriteParallelism());
        if (maxPartitionsDueParallelism >= settings.getMaxTopicPartitionCount()) {
            return settings.getMaxTopicPartitionCount();
        }
        return Math.min(settings.getMaxTopicPartitionCount(), Math.max(
                maxPartitionsDueParallelism,
                calculatePartitionsAccordingLoad(stat.getMessagesPerMinute(), stat.getMessageSize())));
    }

    private int calculatePartitionsAccordingLoad(final int messagesPerMinute, final int avgEventSizeBytes) {
        final float throughoutputMbPerSec = ((float)messagesPerMinute * (float)avgEventSizeBytes) / (1024.f * 1024.f * 60.f);
        return partitionsCalculator.getBestPartitionsCount(avgEventSizeBytes, throughoutputMbPerSec);
    }
}
