package org.zalando.nakadi.repository.kafka;

import com.google.common.base.Preconditions;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import javax.annotation.Nullable;
import kafka.admin.AdminUtils;
import kafka.common.TopicExistsException;
import kafka.utils.ZkUtils;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.InterruptException;
import org.apache.kafka.common.errors.NetworkException;
import org.apache.kafka.common.errors.NotLeaderForPartitionException;
import org.apache.kafka.common.errors.UnknownServerException;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zalando.nakadi.config.NakadiSettings;
import org.zalando.nakadi.domain.BatchItem;
import org.zalando.nakadi.domain.EventPublishingStatus;
import org.zalando.nakadi.domain.EventPublishingStep;
import org.zalando.nakadi.domain.NakadiCursor;
import org.zalando.nakadi.domain.PartitionStatistics;
import org.zalando.nakadi.domain.Timeline;
import org.zalando.nakadi.exceptions.EventPublishingException;
import org.zalando.nakadi.exceptions.InvalidCursorException;
import org.zalando.nakadi.exceptions.ServiceUnavailableException;
import org.zalando.nakadi.exceptions.TopicCreationException;
import org.zalando.nakadi.exceptions.TopicDeletionException;
import org.zalando.nakadi.repository.EventConsumer;
import org.zalando.nakadi.repository.TopicRepository;
import org.zalando.nakadi.repository.zookeeper.ZooKeeperHolder;
import org.zalando.nakadi.repository.zookeeper.ZookeeperSettings;
import org.zalando.nakadi.util.UUIDGenerator;
import static java.util.Collections.unmodifiableList;
import static java.util.stream.Collectors.toList;
import static org.zalando.nakadi.domain.CursorError.NULL_OFFSET;
import static org.zalando.nakadi.domain.CursorError.NULL_PARTITION;
import static org.zalando.nakadi.domain.CursorError.PARTITION_NOT_FOUND;
import static org.zalando.nakadi.domain.CursorError.UNAVAILABLE;

public class KafkaTopicRepository implements TopicRepository {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaTopicRepository.class);

    private final ZooKeeperHolder zkFactory;
    private final KafkaFactory kafkaFactory;
    private final NakadiSettings nakadiSettings;
    private final KafkaSettings kafkaSettings;
    private final ZookeeperSettings zookeeperSettings;
    private final ConcurrentMap<String, HystrixKafkaCircuitBreaker> circuitBreakers;
    private final UUIDGenerator uuidGenerator;

    public KafkaTopicRepository(final ZooKeeperHolder zkFactory,
                                final KafkaFactory kafkaFactory,
                                final NakadiSettings nakadiSettings,
                                final KafkaSettings kafkaSettings,
                                final ZookeeperSettings zookeeperSettings,
                                final UUIDGenerator uuidGenerator) {
        this.zkFactory = zkFactory;
        this.kafkaFactory = kafkaFactory;
        this.nakadiSettings = nakadiSettings;
        this.kafkaSettings = kafkaSettings;
        this.zookeeperSettings = zookeeperSettings;
        this.uuidGenerator = uuidGenerator;
        this.circuitBreakers = new ConcurrentHashMap<>();
    }

    public List<String> listTopics() throws ServiceUnavailableException {
        try {
            return zkFactory.get()
                    .getChildren()
                    .forPath("/brokers/topics");
        } catch (final Exception e) {
            throw new ServiceUnavailableException("Failed to list topics", e);
        }
    }

    @Override
    public String createTopic(final int partitionCount, final Long retentionTimeMs)
            throws TopicCreationException {
        if (retentionTimeMs == null) {
            throw new IllegalArgumentException("Retention time can not be null");
        }
        final String topicName = uuidGenerator.randomUUID().toString();
        createTopic(topicName,
                partitionCount,
                nakadiSettings.getDefaultTopicReplicaFactor(),
                retentionTimeMs,
                nakadiSettings.getDefaultTopicRotationMs());
        return topicName;
    }

    private void createTopic(final String topic, final int partitionsNum, final int replicaFactor,
                             final long retentionMs, final long rotationMs)
            throws TopicCreationException {
        try {
            doWithZkUtils(zkUtils -> {
                final Properties topicConfig = new Properties();
                topicConfig.setProperty("retention.ms", Long.toString(retentionMs));
                topicConfig.setProperty("segment.ms", Long.toString(rotationMs));
                AdminUtils.createTopic(zkUtils, topic, partitionsNum, replicaFactor, topicConfig);
            });
        } catch (final TopicExistsException e) {
            throw new TopicCreationException("Topic with name " + topic +
                    " already exists (or wasn't completely removed yet)", e);
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
                .anyMatch(t -> t.equals(topic));
    }

    private static CompletableFuture<Exception> publishItem(
            final Producer<String, String> producer,
            final String topicId,
            final BatchItem item,
            final HystrixKafkaCircuitBreaker circuitBreaker) throws EventPublishingException {
        try {
            final CompletableFuture<Exception> result = new CompletableFuture<>();
            final ProducerRecord<String, String> kafkaRecord = new ProducerRecord<>(
                    topicId,
                    KafkaCursor.toKafkaPartition(item.getPartition()),
                    item.getPartition(),
                    item.getEvent().toString());

            circuitBreaker.markStart();
            producer.send(kafkaRecord, ((metadata, exception) -> {
                if (null != exception) {
                    LOG.warn("Failed to publish to kafka topic {}", topicId, exception);
                    item.updateStatusAndDetail(EventPublishingStatus.FAILED, "internal error");
                    if (hasKafkaConnectionException(exception)) {
                        circuitBreaker.markFailure();
                    } else {
                        circuitBreaker.markSuccessfully();
                    }
                    result.complete(exception);
                } else {
                    item.updateStatusAndDetail(EventPublishingStatus.SUBMITTED, "");
                    circuitBreaker.markSuccessfully();
                    result.complete(null);
                }
            }));
            return result;
        } catch (final InterruptException e) {
            Thread.currentThread().interrupt();
            circuitBreaker.markSuccessfully();
            item.updateStatusAndDetail(EventPublishingStatus.FAILED, "internal error");
            throw new EventPublishingException("Error publishing message to kafka", e);
        } catch (final RuntimeException e) {
            circuitBreaker.markSuccessfully();
            item.updateStatusAndDetail(EventPublishingStatus.FAILED, "internal error");
            throw new EventPublishingException("Error publishing message to kafka", e);
        }
    }

    private static boolean isExceptionShouldLeadToReset(@Nullable final Exception exception) {
        if (null == exception) {
            return false;
        }
        return Stream.of(NotLeaderForPartitionException.class, UnknownTopicOrPartitionException.class)
                .anyMatch(clazz -> clazz.isAssignableFrom(exception.getClass()));
    }

    private static boolean hasKafkaConnectionException(final Exception exception) {
        return exception instanceof org.apache.kafka.common.errors.TimeoutException ||
                exception instanceof NetworkException ||
                exception instanceof UnknownServerException;
    }

    @Override
    public void syncPostBatch(final String topicId, final List<BatchItem> batch) throws EventPublishingException {
        final Producer<String, String> producer = kafkaFactory.takeProducer();
        try {
            final Map<String, String> partitionToBroker = producer.partitionsFor(topicId).stream().collect(
                    Collectors.toMap(p -> String.valueOf(p.partition()), p -> String.valueOf(p.leader().id())));
            batch.forEach(item -> {
                Preconditions.checkNotNull(
                        item.getPartition(), "BatchItem partition can't be null at the moment of publishing!");
                item.setBrokerId(partitionToBroker.get(item.getPartition()));
            });

            int shortCircuited = 0;
            final Map<BatchItem, CompletableFuture<Exception>> sendFutures = new HashMap<>();
            for (final BatchItem item : batch) {
                item.setStep(EventPublishingStep.PUBLISHING);
                final HystrixKafkaCircuitBreaker circuitBreaker = circuitBreakers.computeIfAbsent(
                        item.getBrokerId(), brokerId -> new HystrixKafkaCircuitBreaker(brokerId));
                if (circuitBreaker.allowRequest()) {
                    sendFutures.put(item, publishItem(producer, topicId, item, circuitBreaker));
                } else {
                    shortCircuited++;
                    item.updateStatusAndDetail(EventPublishingStatus.FAILED, "short circuited");
                }
            }
            if (shortCircuited > 0) {
                LOG.warn("Short circuiting request to Kafka {} time(s) due to timeout for topic {}",
                        shortCircuited, topicId);
            }
            final CompletableFuture<Void> multiFuture = CompletableFuture.allOf(
                    sendFutures.values().toArray(new CompletableFuture<?>[sendFutures.size()]));
            multiFuture.get(createSendTimeout(), TimeUnit.MILLISECONDS);

            // Now lets check for errors
            final Optional<Exception> needReset = sendFutures.entrySet().stream()
                    .filter(entry -> isExceptionShouldLeadToReset(entry.getValue().getNow(null)))
                    .map(entry -> entry.getValue().getNow(null))
                    .findAny();
            if (needReset.isPresent()) {
                LOG.info("Terminating producer while publishing to topic {} because of unrecoverable exception",
                        topicId, needReset.get());
                kafkaFactory.terminateProducer(producer);
            }
        } catch (final TimeoutException ex) {
            failUnpublished(batch, "timed out");
            throw new EventPublishingException("Error publishing message to kafka", ex);
        } catch (final ExecutionException ex) {
            failUnpublished(batch, "internal error");
            throw new EventPublishingException("Error publishing message to kafka", ex);
        } catch (final InterruptedException ex) {
            Thread.currentThread().interrupt();
            failUnpublished(batch, "interrupted");
            throw new EventPublishingException("Error publishing message to kafka", ex);
        } finally {
            kafkaFactory.releaseProducer(producer);
        }
        final boolean atLeastOneFailed = batch.stream()
                .anyMatch(item -> item.getResponse().getPublishingStatus() == EventPublishingStatus.FAILED);
        if (atLeastOneFailed) {
            failUnpublished(batch, "internal error");
            throw new EventPublishingException("Error publishing message to kafka");
        }
    }

    private long createSendTimeout() {
        return nakadiSettings.getKafkaSendTimeoutMs() + kafkaSettings.getRequestTimeoutMs();
    }

    private void failUnpublished(final List<BatchItem> batch, final String reason) {
        batch.stream()
                .filter(item -> item.getResponse().getPublishingStatus() != EventPublishingStatus.SUBMITTED)
                .filter(item -> item.getResponse().getDetail().isEmpty())
                .forEach(item -> item.updateStatusAndDetail(EventPublishingStatus.FAILED, reason));
    }

    @Override
    public Optional<PartitionStatistics> loadPartitionStatistics(final Timeline timeline, final String partition)
            throws ServiceUnavailableException {
        try (Consumer<String, String> consumer = kafkaFactory.getConsumer()) {
            final Optional<PartitionInfo> tp = consumer.partitionsFor(timeline.getTopic()).stream()
                    .filter(p -> KafkaCursor.toNakadiPartition(p.partition()).equals(partition))
                    .findAny();
            if (!tp.isPresent()) {
                return Optional.empty();
            }
            final TopicPartition kafkaTP = tp.map(v -> new TopicPartition(v.topic(), v.partition())).get();
            consumer.assign(Collections.singletonList(kafkaTP));
            consumer.seekToBeginning(kafkaTP);

            final long begin = consumer.position(kafkaTP);
            consumer.seekToEnd(kafkaTP);
            final long end = consumer.position(kafkaTP);

            return Optional.of(new KafkaPartitionStatistics(timeline, kafkaTP.partition(), begin, end - 1));
        } catch (final Exception e) {
            throw new ServiceUnavailableException("Error occurred when fetching partitions offsets", e);
        }
    }

    @Override
    public List<PartitionStatistics> loadTopicStatistics(final Collection<Timeline> timelines)
            throws ServiceUnavailableException {
        try (Consumer<String, String> consumer = kafkaFactory.getConsumer()) {
            final Map<TopicPartition, Timeline> backMap = new HashMap<>();
            for (final Timeline timeline : timelines) {
                consumer.partitionsFor(timeline.getTopic())
                        .stream()
                        .map(p -> new TopicPartition(p.topic(), p.partition()))
                        .forEach(tp -> backMap.put(tp, timeline));
            }
            final TopicPartition[] kafkaTPs = backMap.keySet().toArray(new TopicPartition[backMap.size()]);
            consumer.assign(Arrays.asList(kafkaTPs));
            consumer.seekToBeginning(kafkaTPs);
            final long[] begins = Stream.of(kafkaTPs).mapToLong(consumer::position).toArray();

            consumer.seekToEnd(kafkaTPs);
            final long[] ends = Stream.of(kafkaTPs).mapToLong(consumer::position).toArray();

            return IntStream.range(0, kafkaTPs.length)
                    .mapToObj(i -> new KafkaPartitionStatistics(
                            backMap.get(kafkaTPs[i]),
                            kafkaTPs[i].partition(),
                            begins[i],
                            ends[i] - 1))
                    .collect(toList());
        } catch (final Exception e) {
            throw new ServiceUnavailableException("Error occurred when fetching partitions offsets", e);
        }
    }

    @Override
    public List<String> listPartitionNames(final String topicId) {
        final Producer<String, String> producer = kafkaFactory.takeProducer();
        try {
            return unmodifiableList(producer.partitionsFor(topicId)
                    .stream()
                    .map(partitionInfo -> KafkaCursor.toNakadiPartition(partitionInfo.partition()))
                    .collect(toList()));
        } finally {
            kafkaFactory.releaseProducer(producer);
        }
    }

    public Consumer<String, String> createKafkaConsumer() {
        return kafkaFactory.getConsumer();
    }

    @Override
    public EventConsumer createEventConsumer(final String clientId, final List<NakadiCursor> cursors)
            throws ServiceUnavailableException, InvalidCursorException {

        final Map<NakadiCursor, KafkaCursor> cursorMapping = this.convertToKafkaCursors(cursors);
        final Map<TopicPartition, Timeline> timelineMap = cursorMapping.entrySet().stream()
                .collect(Collectors.toMap(
                        entry -> new TopicPartition(entry.getValue().getTopic(), entry.getValue().getPartition()),
                        entry -> entry.getKey().getTimeline(),
                        (v1, v2) -> v2));
        final List<KafkaCursor> kafkaCursors = cursorMapping.values().stream()
                .map(kafkaCursor -> kafkaCursor.addOffset(1))
                .collect(toList());

        return new NakadiKafkaConsumer(
                kafkaFactory.getConsumer(clientId),
                kafkaCursors,
                timelineMap,
                nakadiSettings.getKafkaPollTimeoutMs());

    }

    public int compareOffsets(final NakadiCursor first, final NakadiCursor second) throws InvalidCursorException {
        return KafkaCursor.fromNakadiCursor(first).compareTo(KafkaCursor.fromNakadiCursor(second));
    }

    public void validateReadCursors(final List<NakadiCursor> cursors)
            throws InvalidCursorException, ServiceUnavailableException {
        convertToKafkaCursors(cursors);
    }

    private Map<NakadiCursor, KafkaCursor> convertToKafkaCursors(final List<NakadiCursor> cursors)
            throws ServiceUnavailableException, InvalidCursorException {
        final List<Timeline> timelines = cursors.stream().map(NakadiCursor::getTimeline).distinct().collect(toList());
        final List<PartitionStatistics> statistics = loadTopicStatistics(timelines);

        final Map<NakadiCursor, KafkaCursor> result = new HashMap<>();
        for (final NakadiCursor position : cursors) {
            validateCursorForNulls(position);
            final Optional<PartitionStatistics> partition =
                    statistics.stream().filter(t -> Objects.equals(t.getPartition(), position.getPartition()))
                            .filter(t -> Objects.equals(t.getTimeline().getTopic(), position.getTopic()))
                            .findAny();
            if (!partition.isPresent()) {
                throw new InvalidCursorException(PARTITION_NOT_FOUND, position);
            }
            final KafkaCursor toCheck = KafkaCursor.fromNakadiCursor(position);

            // Checking oldest position
            final KafkaCursor oldestCursor = KafkaCursor.fromNakadiCursor(partition.get().getBeforeFirst());
            if (toCheck.compareTo(oldestCursor) < 0) {
                throw new InvalidCursorException(UNAVAILABLE, position);
            }
            // checking newest position
            final KafkaCursor newestPosition = KafkaCursor.fromNakadiCursor(partition.get().getLast());
            if (toCheck.compareTo(newestPosition) > 0) {
                throw new InvalidCursorException(UNAVAILABLE, position);
            } else {
                result.put(position, toCheck);
            }
        }
        return result;
    }

    @Override
    public void validateCommitCursor(final NakadiCursor position) throws InvalidCursorException {
        KafkaCursor.fromNakadiCursor(position);
    }

    private void validateCursorForNulls(final NakadiCursor cursor) throws InvalidCursorException {
        if (cursor.getPartition() == null) {
            throw new InvalidCursorException(NULL_PARTITION, cursor);
        }
        if (cursor.getOffset() == null) {
            throw new InvalidCursorException(NULL_OFFSET, cursor);
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
            zkUtils = ZkUtils.apply(connectionString, zookeeperSettings.getZkSessionTimeoutMs(),
                    zookeeperSettings.getZkConnectionTimeoutMs(), false);
            action.execute(zkUtils);
        } finally {
            if (zkUtils != null) {
                zkUtils.close();
            }
        }
    }
}
