package org.zalando.nakadi.repository.kafka;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import io.opentracing.Scope;
import io.opentracing.Span;
import io.opentracing.tag.Tags;
import io.opentracing.util.GlobalTracer;
import kafka.admin.AdminUtils;
import kafka.server.ConfigType;
import kafka.utils.ZkUtils;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewPartitions;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.InterruptException;
import org.apache.kafka.common.errors.NetworkException;
import org.apache.kafka.common.errors.NotLeaderForPartitionException;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.errors.UnknownServerException;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.echocat.jomon.runtime.concurrent.RetryForSpecifiedCountStrategy;
import org.echocat.jomon.runtime.concurrent.RetryForSpecifiedTimeStrategy;
import org.echocat.jomon.runtime.concurrent.Retryer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zalando.nakadi.config.NakadiSettings;
import org.zalando.nakadi.domain.BatchItem;
import org.zalando.nakadi.domain.EventPublishingStatus;
import org.zalando.nakadi.domain.EventPublishingStep;
import org.zalando.nakadi.domain.NakadiCursor;
import org.zalando.nakadi.domain.PartitionEndStatistics;
import org.zalando.nakadi.domain.PartitionStatistics;
import org.zalando.nakadi.domain.Timeline;
import org.zalando.nakadi.exceptions.runtime.CannotAddPartitionToTopicException;
import org.zalando.nakadi.exceptions.runtime.EventPublishingException;
import org.zalando.nakadi.exceptions.runtime.InvalidCursorException;
import org.zalando.nakadi.exceptions.runtime.ServiceTemporarilyUnavailableException;
import org.zalando.nakadi.exceptions.runtime.TopicConfigException;
import org.zalando.nakadi.exceptions.runtime.TopicCreationException;
import org.zalando.nakadi.exceptions.runtime.TopicDeletionException;
import org.zalando.nakadi.exceptions.runtime.TopicRepositoryException;
import org.zalando.nakadi.repository.EventConsumer;
import org.zalando.nakadi.repository.NakadiTopicConfig;
import org.zalando.nakadi.repository.TopicRepository;
import org.zalando.nakadi.repository.zookeeper.ZookeeperSettings;
import org.zalando.nakadi.service.TracingService;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static com.google.common.collect.Lists.newArrayList;
import static java.util.Collections.unmodifiableList;
import static java.util.stream.Collectors.toList;
import static org.zalando.nakadi.domain.CursorError.NULL_OFFSET;
import static org.zalando.nakadi.domain.CursorError.NULL_PARTITION;
import static org.zalando.nakadi.domain.CursorError.PARTITION_NOT_FOUND;
import static org.zalando.nakadi.domain.CursorError.UNAVAILABLE;

public class KafkaTopicRepository implements TopicRepository {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaTopicRepository.class);

    private final KafkaZookeeper kafkaZookeeper;
    private final KafkaFactory kafkaFactory;
    private final NakadiSettings nakadiSettings;
    private final KafkaSettings kafkaSettings;
    private final ZookeeperSettings zookeeperSettings;
    private final ConcurrentMap<String, HystrixKafkaCircuitBreaker> circuitBreakers;
    private final KafkaTopicConfigFactory kafkaTopicConfigFactory;
    private final KafkaLocationManager kafkaLocationManager;

    public KafkaTopicRepository(final KafkaZookeeper kafkaZookeeper,
                                final KafkaFactory kafkaFactory,
                                final NakadiSettings nakadiSettings,
                                final KafkaSettings kafkaSettings,
                                final ZookeeperSettings zookeeperSettings,
                                final KafkaTopicConfigFactory kafkaTopicConfigFactory,
                                final KafkaLocationManager kafkaLocationManager) {
        this.kafkaZookeeper = kafkaZookeeper;
        this.kafkaFactory = kafkaFactory;
        this.nakadiSettings = nakadiSettings;
        this.kafkaSettings = kafkaSettings;
        this.zookeeperSettings = zookeeperSettings;
        this.kafkaLocationManager = kafkaLocationManager;
        this.kafkaTopicConfigFactory = kafkaTopicConfigFactory;
        this.circuitBreakers = new ConcurrentHashMap<>();
    }

    private CompletableFuture<Exception> publishItem(
            final Producer<String, String> producer,
            final String topicId,
            final BatchItem item,
            final HystrixKafkaCircuitBreaker circuitBreaker,
            final Span currentSpan) throws EventPublishingException {
        final Scope publishScope = TracingService.activateSpan(currentSpan, false);
        try {
            final CompletableFuture<Exception> result = new CompletableFuture<>();
            final ProducerRecord<String, String> kafkaRecord = new ProducerRecord<>(
                    topicId,
                    KafkaCursor.toKafkaPartition(item.getPartition()),
                    item.getEventKey(),
                    item.dumpEventToString());

            circuitBreaker.markStart();
            producer.send(kafkaRecord, ((metadata, exception) -> {
                final Scope producerScope = TracingService.activateSpan(currentSpan, false);
                if (null != exception) {
                    LOG.warn("Failed to publish to kafka topic {}", topicId, exception);
                    handlePublishingException(producerScope, item, "internal error");
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
            handlePublishingException(publishScope, item, "internal error");
            throw new EventPublishingException("Error publishing message to kafka", e);
        } catch (final RuntimeException e) {
            kafkaFactory.terminateProducer(producer);
            circuitBreaker.markSuccessfully();
            handlePublishingException(publishScope, item, "internal error");
            throw new EventPublishingException("Error publishing message to kafka", e);
        }
    }

    private void handlePublishingException(final Scope scope, final BatchItem batchItem, final String error) {
        batchItem.updateStatusAndDetail(EventPublishingStatus.FAILED, error);
        if (batchItem.getEvent().has("metadata")) {
            scope.span().log(
                    ImmutableMap.of("event.id", batchItem.getEvent().getJSONObject("metadata")
                            .getString("eid"), "error", error));
        }
    }


    private static boolean isExceptionShouldLeadToReset(@Nullable final Exception exception) {
        if (null == exception) {
            return false;
        }
        return Stream.of(NotLeaderForPartitionException.class, UnknownTopicOrPartitionException.class,
                org.apache.kafka.common.errors.TimeoutException.class, NetworkException.class,
                UnknownServerException.class)
                .anyMatch(clazz -> clazz.isAssignableFrom(exception.getClass()));
    }

    private static boolean hasKafkaConnectionException(final Exception exception) {
        return exception instanceof org.apache.kafka.common.errors.TimeoutException ||
                exception instanceof NetworkException ||
                exception instanceof UnknownServerException;
    }

    public List<String> listTopics() throws TopicRepositoryException {
        try {
            return kafkaZookeeper.listTopics();
        } catch (final Exception e) {
            throw new TopicRepositoryException("Failed to list topics", e);
        }
    }

    @Override
    public void repartition(final String topic, final int partitionsNumber) throws CannotAddPartitionToTopicException,
            TopicConfigException {
        try (AdminClient adminClient = AdminClient.create(kafkaLocationManager.getProperties())) {
            adminClient.createPartitions(ImmutableMap.of(topic, NewPartitions.increaseTo(partitionsNumber)));
            final long timeoutMillis = TimeUnit.SECONDS.toMillis(5);
            final Boolean areNewPartitionsAdded = Retryer.executeWithRetry(() -> {
                        try (Consumer<byte[], byte[]> consumer = kafkaFactory.getConsumer()) {
                            return consumer.partitionsFor(topic).size() == partitionsNumber;
                        }
                    },
                    new RetryForSpecifiedTimeStrategy<Boolean>(timeoutMillis)
                            .withWaitBetweenEachTry(100L)
                            .withResultsThatForceRetry(Boolean.FALSE));
            if (!Boolean.TRUE.equals(areNewPartitionsAdded)) {
                throw new TopicConfigException(String.format("Failed to repartition topic to %s", partitionsNumber));
            }
            final Producer<String, String> producer = kafkaFactory.takeProducer();
            kafkaFactory.terminateProducer(producer);
            kafkaFactory.releaseProducer(producer);
        } catch (Exception e) {
            throw new CannotAddPartitionToTopicException(String
                    .format("Failed to increase the number of partition for %s topic to %s", topic,
                            partitionsNumber), e);
        }
    }

    @Override
    public String createTopic(final NakadiTopicConfig nakadiTopicConfig) throws TopicCreationException {

        final KafkaTopicConfig kafkaTopicConfig = kafkaTopicConfigFactory.createKafkaTopicConfig(nakadiTopicConfig);
        try {
            doWithZkUtils(zkUtils -> {
                AdminUtils.createTopic(zkUtils,
                        kafkaTopicConfig.getTopicName(),
                        kafkaTopicConfig.getPartitionCount(),
                        kafkaTopicConfig.getReplicaFactor(),
                        kafkaTopicConfigFactory.createKafkaTopicLevelProperties(kafkaTopicConfig),
                        kafkaTopicConfig.getRackAwareMode());
            });
        } catch (final TopicExistsException e) {
            throw new TopicCreationException("Topic with name " + kafkaTopicConfig.getTopicName() +
                    " already exists (or wasn't completely removed yet)", e);
        } catch (final Exception e) {
            throw new TopicCreationException("Unable to create topic " + kafkaTopicConfig.getTopicName(), e);
        }
        // Next step is to wait for topic initialization. On can not skip this task, cause kafka instances may not
        // receive information about topic creation, which in turn will block publishing.
        // This kind of behavior was observed during tests, but may also present on highly loaded event types.
        final long timeoutMillis = TimeUnit.SECONDS.toMillis(5);
        final Boolean allowsConsumption = Retryer.executeWithRetry(() -> {
                    try (Consumer<byte[], byte[]> consumer = kafkaFactory.getConsumer()) {
                        return null != consumer.partitionsFor(kafkaTopicConfig.getTopicName());
                    }
                },
                new RetryForSpecifiedTimeStrategy<Boolean>(timeoutMillis)
                        .withWaitBetweenEachTry(100L)
                        .withResultsThatForceRetry(Boolean.FALSE));
        if (!Boolean.TRUE.equals(allowsConsumption)) {
            throw new TopicCreationException("Failed to confirm topic creation within " + timeoutMillis + " millis");
        }

        return kafkaTopicConfig.getTopicName();
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
    public boolean topicExists(final String topic) throws TopicRepositoryException {
        return listTopics()
                .stream()
                .anyMatch(t -> t.equals(topic));
    }

    @Override
    public void syncPostBatch(final String topicId, final List<BatchItem> batch, final Span parentSpan,
                              final String eventType)
            throws EventPublishingException {
        final Span publishingSpan = GlobalTracer.get()
                .buildSpan("publishing_to_kafka")
                .asChildOf(parentSpan)
                .start();
        publishingSpan.setTag("event_type", eventType);
        Tags.MESSAGE_BUS_DESTINATION.set(publishingSpan, topicId);
        try (Scope kafkaPublishingScope = TracingService.activateSpan(publishingSpan, false)) {
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
                        sendFutures.put(item, publishItem(producer, topicId, item, circuitBreaker, publishingSpan));
                    } else {
                        shortCircuited++;
                        handlePublishingException(kafkaPublishingScope, item, "short circuited");
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
                kafkaFactory.terminateProducer(producer);
                failUnpublished(batch, "timed out");
                TracingService.setCustomTags(kafkaPublishingScope, ImmutableMap.<String, Object>builder()
                        .put("failure_reason", "timed out").build());
                TracingService.logErrorInSpan(kafkaPublishingScope, ex.getMessage());
                throw new EventPublishingException("Error publishing message to kafka", ex);
            } catch (final ExecutionException ex) {
                failUnpublished(batch, "internal error");
                TracingService.setCustomTags(kafkaPublishingScope, ImmutableMap.<String, Object>builder()
                        .put("failure_reason", "internal error").build());
                TracingService.logErrorInSpan(kafkaPublishingScope, ex.getMessage());
                throw new EventPublishingException("Error publishing message to kafka", ex);
            } catch (final InterruptedException ex) {
                Thread.currentThread().interrupt();
                failUnpublished(batch, "interrupted");
                TracingService.setCustomTags(kafkaPublishingScope, ImmutableMap.<String, Object>builder()
                        .put("failure_reason", "interrupted").build());
                TracingService.logErrorInSpan(kafkaPublishingScope, ex.getMessage());
                throw new EventPublishingException("Error publishing message to kafka", ex);
            } finally {
                kafkaFactory.releaseProducer(producer);
            }
            final boolean atLeastOneFailed = batch.stream()
                    .anyMatch(item -> item.getResponse().getPublishingStatus() == EventPublishingStatus.FAILED);
            if (atLeastOneFailed) {
                failUnpublished(batch, "internal error");
                TracingService.setCustomTags(kafkaPublishingScope, ImmutableMap.<String, Object>builder()
                        .put("failure_reason", "internal error").build());
                TracingService.logErrorInSpan(kafkaPublishingScope, "Internal Kafka Error");
                throw new EventPublishingException("Error publishing message to kafka");
            }
        } finally {
            publishingSpan.finish();
        }
    }

    private long createSendTimeout() {
        return nakadiSettings.getKafkaSendTimeoutMs() + kafkaSettings.getRequestTimeoutMs();
    }

    private void failUnpublished(final List<BatchItem> batch, final String reason) {
        logFailedEvents(batch);

        batch.stream()
                .filter(item -> item.getResponse().getPublishingStatus() != EventPublishingStatus.SUBMITTED)
                .filter(item -> item.getResponse().getDetail().isEmpty())
                .forEach(item -> item.updateStatusAndDetail(EventPublishingStatus.FAILED, reason));
    }

    private void logFailedEvents(final List<BatchItem> batch) {
        final Map<String, List<Integer>> result = new HashMap<>();
        for (final BatchItem batchItem : batch) {
            List<Integer> events = result.get(batchItem.getPartition());
            if (events == null) {
                events = new LinkedList<>();
                result.put(batchItem.getPartition(), events);
            }
            if (batchItem.getResponse().getPublishingStatus() == EventPublishingStatus.SUBMITTED) {
                events.add(1);
            } else {
                events.add(0);
            }
        }

        final StringBuilder sb = new StringBuilder();
        for (final Map.Entry<String, List<Integer>> entry : result.entrySet()) {
            sb.append(entry.getKey()).append(":").append(Arrays.toString(entry.getValue().toArray())).append(" ");
        }

        LOG.info("Failed events in batch {}", sb.toString());
    }

    @Override
    public Optional<PartitionStatistics> loadPartitionStatistics(final Timeline timeline, final String partition)
            throws ServiceTemporarilyUnavailableException {
        return loadPartitionStatistics(Collections.singletonList(new TimelinePartition(timeline, partition))).get(0);
    }

    @Override
    public List<Optional<PartitionStatistics>> loadPartitionStatistics(
            final Collection<TimelinePartition> partitions) throws ServiceTemporarilyUnavailableException {
        try {
            return Retryer.executeWithRetry(() -> {
                        return loadPartitionStatisticsInternal(partitions);
                    },
                    new RetryForSpecifiedCountStrategy(3)
                            .withWaitBetweenEachTry(5000)
                            .withExceptionsThatForceRetry(org.apache.kafka.common.errors.TimeoutException.class));
        } catch (final RuntimeException e) {
            throw new ServiceTemporarilyUnavailableException("Error occurred when fetching partitions offsets", e);
        }
    }

    public List<Optional<PartitionStatistics>> loadPartitionStatisticsInternal(
            final Collection<TimelinePartition> partitions) {
        final Map<String, Set<String>> topicToPartitions = partitions.stream().collect(
                Collectors.groupingBy(
                        tp -> tp.getTimeline().getTopic(),
                        Collectors.mapping(TimelinePartition::getPartition, Collectors.toSet())
                ));
        try (Consumer<byte[], byte[]> consumer = kafkaFactory.getConsumer()) {
            final List<PartitionInfo> allKafkaPartitions = topicToPartitions.keySet().stream()
                    .map(consumer::partitionsFor)
                    .flatMap(Collection::stream)
                    .collect(Collectors.toList());
            final List<TopicPartition> partitionsToQuery = allKafkaPartitions.stream()
                    .filter(pi -> topicToPartitions.get(pi.topic())
                            .contains(KafkaCursor.toNakadiPartition(pi.partition())))
                    .map(pi -> new TopicPartition(pi.topic(), pi.partition()))
                    .collect(Collectors.toList());

            consumer.assign(partitionsToQuery);
            consumer.seekToBeginning(partitionsToQuery);
            final List<Long> begins = partitionsToQuery.stream().map(consumer::position).collect(toList());
            consumer.seekToEnd(partitionsToQuery);
            final List<Long> ends = partitionsToQuery.stream().map(consumer::position).collect(toList());

            final List<Optional<PartitionStatistics>> result = new ArrayList<>(partitions.size());
            for (final TimelinePartition tap : partitions) {
                // Now search for an index.
                final Optional<PartitionStatistics> itemResult = IntStream.range(0, partitionsToQuery.size())
                        .filter(i -> {
                            final TopicPartition info = partitionsToQuery.get(i);
                            return info.topic().equals(tap.getTimeline().getTopic()) &&
                                    info.partition() == KafkaCursor.toKafkaPartition(tap.getPartition());
                        }).mapToObj(indexFound -> (PartitionStatistics) new KafkaPartitionStatistics(
                                tap.getTimeline(),
                                partitionsToQuery.get(indexFound).partition(),
                                begins.get(indexFound),
                                ends.get(indexFound) - 1L))
                        .findAny();
                result.add(itemResult);
            }
            return result;
        }
    }

    @Override
    public List<PartitionStatistics> loadTopicStatistics(final Collection<Timeline> timelines)
            throws ServiceTemporarilyUnavailableException {
        try {
            return Retryer.executeWithRetry(() -> {
                        return loadTopicStatisticsInternal(timelines);
                    },
                    new RetryForSpecifiedCountStrategy(3)
                            .withWaitBetweenEachTry(5000)
                            .withExceptionsThatForceRetry(org.apache.kafka.common.errors.TimeoutException.class));
        } catch (final RuntimeException e) {
            throw new ServiceTemporarilyUnavailableException("Error occurred when fetching partitions offsets", e);
        }
    }

    public List<PartitionStatistics> loadTopicStatisticsInternal(final Collection<Timeline> timelines) {
        try (Consumer<byte[], byte[]> consumer = kafkaFactory.getConsumer()) {
            final Map<TopicPartition, Timeline> backMap = new HashMap<>();
            for (final Timeline timeline : timelines) {
                consumer.partitionsFor(timeline.getTopic())
                        .stream()
                        .map(p -> new TopicPartition(p.topic(), p.partition()))
                        .forEach(tp -> backMap.put(tp, timeline));
            }
            final List<TopicPartition> kafkaTPs = new ArrayList<>(backMap.keySet());
            consumer.assign(kafkaTPs);
            consumer.seekToBeginning(kafkaTPs);
            final long[] begins = kafkaTPs.stream().mapToLong(consumer::position).toArray();

            consumer.seekToEnd(kafkaTPs);
            final long[] ends = kafkaTPs.stream().mapToLong(consumer::position).toArray();

            return IntStream.range(0, kafkaTPs.size())
                    .mapToObj(i -> new KafkaPartitionStatistics(
                            backMap.get(kafkaTPs.get(i)),
                            kafkaTPs.get(i).partition(),
                            begins[i],
                            ends[i] - 1))
                    .collect(toList());
        }
    }

    @Override
    public List<PartitionEndStatistics> loadTopicEndStatistics(final Collection<Timeline> timelines)
            throws ServiceTemporarilyUnavailableException {
        try {
            return Retryer.executeWithRetry(() -> {
                        return loadTopicEndStatisticsInternal(timelines);
                    },
                    new RetryForSpecifiedCountStrategy(3)
                            .withWaitBetweenEachTry(5000)
                            .withExceptionsThatForceRetry(org.apache.kafka.common.errors.TimeoutException.class));
        } catch (final RuntimeException e) {
            throw new ServiceTemporarilyUnavailableException("Error occurred when fetching partitions offsets", e);
        }
    }

    private List<PartitionEndStatistics> loadTopicEndStatisticsInternal(final Collection<Timeline> timelines) {
        try (Consumer<byte[], byte[]> consumer = kafkaFactory.getConsumer()) {
            final Map<TopicPartition, Timeline> backMap = new HashMap<>();
            for (final Timeline timeline : timelines) {
                consumer.partitionsFor(timeline.getTopic())
                        .stream()
                        .map(p -> new TopicPartition(p.topic(), p.partition()))
                        .forEach(tp -> backMap.put(tp, timeline));
            }
            final List<TopicPartition> kafkaTPs = newArrayList(backMap.keySet());
            consumer.assign(kafkaTPs);
            consumer.seekToEnd(kafkaTPs);
            return backMap.entrySet().stream()
                    .map(e -> {
                        final TopicPartition tp = e.getKey();
                        final Timeline timeline = e.getValue();
                        return new KafkaPartitionEndStatistics(timeline, tp.partition(), consumer.position(tp) - 1);
                    })
                    .collect(toList());
        }
    }

    @Override
    public List<String> listPartitionNames(final String topicId) {
        return Retryer.executeWithRetry(() -> {
                    return listPartitionNamesInternal(topicId);
                },
                new RetryForSpecifiedCountStrategy(3)
                        .withWaitBetweenEachTry(5000)
                        .withExceptionsThatForceRetry(org.apache.kafka.common.errors.TimeoutException.class));
    }

    @Override
    public Map<org.zalando.nakadi.domain.TopicPartition, Long> getSizeStats() {
        final Map<org.zalando.nakadi.domain.TopicPartition, Long> result = new HashMap<>();

        try {
            final List<String> brokers = kafkaZookeeper.getBrokerIdsForSizeStats();

            for (final String brokerId : brokers) {
                final BubukuSizeStats stats = kafkaZookeeper.getSizeStatsForBroker(brokerId);
                stats.getPerPartitionStats().forEach((topic, partitionSizes) -> {
                    partitionSizes.forEach((partition, size) -> {
                        final org.zalando.nakadi.domain.TopicPartition tp =
                                new org.zalando.nakadi.domain.TopicPartition(topic, partition);

                        result.compute(tp, (ignore, oldSize) ->
                                Optional.ofNullable(oldSize).map(v -> Math.max(oldSize, size)).orElse(size));
                    });
                });
            }
            return result;
        } catch (Exception e) {
            throw new RuntimeException("Failed to acquire size statistics", e);
        }
    }

    public List<String> listPartitionNamesInternal(final String topicId) {
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

    @Override
    public EventConsumer.LowLevelConsumer createEventConsumer(
            @Nullable final String clientId, final List<NakadiCursor> cursors)
            throws ServiceTemporarilyUnavailableException, InvalidCursorException {

        final Map<NakadiCursor, KafkaCursor> cursorMapping = convertToKafkaCursors(cursors);
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

    @Override
    public void validateReadCursors(final List<NakadiCursor> cursors)
            throws InvalidCursorException, ServiceTemporarilyUnavailableException {
        convertToKafkaCursors(cursors);
    }

    private Map<NakadiCursor, KafkaCursor> convertToKafkaCursors(final List<NakadiCursor> cursors)
            throws ServiceTemporarilyUnavailableException, InvalidCursorException {
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
            final KafkaCursor toCheck = position.asKafkaCursor();

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
    public void setRetentionTime(final String topic, final Long retentionMs) throws TopicConfigException {
        try {
            doWithZkUtils(zkUtils -> {
                final Properties topicProps = AdminUtils.fetchEntityConfig(zkUtils, ConfigType.Topic(), topic);
                topicProps.setProperty("retention.ms", Long.toString(retentionMs));
                AdminUtils.changeTopicConfig(zkUtils, topic, topicProps);
            });
        } catch (final Exception e) {
            throw new TopicConfigException("Unable to update retention time for topic " + topic, e);
        }
    }

    private void validateCursorForNulls(final NakadiCursor cursor) throws InvalidCursorException {
        if (cursor.getPartition() == null) {
            throw new InvalidCursorException(NULL_PARTITION, cursor);
        }
        if (cursor.getOffset() == null) {
            throw new InvalidCursorException(NULL_OFFSET, cursor);
        }
    }

    private void doWithZkUtils(final ZkUtilsAction action) throws Exception {
        ZkUtils zkUtils = null;
        try {
            zkUtils = ZkUtils.apply(
                    kafkaZookeeper.getZookeeperConnectionString(),
                    zookeeperSettings.getZkSessionTimeoutMs(),
                    zookeeperSettings.getZkConnectionTimeoutMs(),
                    false);
            action.execute(zkUtils);
        } finally {
            if (zkUtils != null) {
                zkUtils.close();
            }
        }
    }

    @FunctionalInterface
    private interface ZkUtilsAction {
        void execute(ZkUtils zkUtils) throws Exception;
    }
}
