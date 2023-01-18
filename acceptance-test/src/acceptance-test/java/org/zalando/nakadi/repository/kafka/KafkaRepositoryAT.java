package org.zalando.nakadi.repository.kafka;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.echocat.jomon.runtime.concurrent.RetryForSpecifiedTimeStrategy;
import org.hamcrest.Matchers;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.zalando.nakadi.config.NakadiSettings;
import org.zalando.nakadi.domain.BatchFactory;
import org.zalando.nakadi.domain.BatchItem;
import org.zalando.nakadi.domain.CleanupPolicy;
import org.zalando.nakadi.domain.EventOwnerHeader;
import org.zalando.nakadi.domain.EventPublishingStatus;
import org.zalando.nakadi.repository.NakadiTopicConfig;
import org.zalando.nakadi.util.UUIDGenerator;
import org.zalando.nakadi.utils.TestUtils;
import org.zalando.nakadi.webservice.BaseAT;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import static org.echocat.jomon.runtime.concurrent.Retryer.executeWithRetry;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.arrayWithSize;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasSize;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.zalando.nakadi.repository.kafka.KafkaTestHelper.createKafkaProperties;

public class KafkaRepositoryAT extends BaseAT {

    private static final int DEFAULT_PARTITION_COUNT = 8;
    private static final int DEFAULT_REPLICA_FACTOR = 1;
    private static final int MAX_TOPIC_PARTITION_COUNT = 10;
    private static final int DEFAULT_TOPIC_ROTATION = 50000000;
    private static final int COMPACTED_TOPIC_ROTATION = 60000;
    private static final int COMPACTED_TOPIC_SEGMENT_BYTES = 1000000;
    private static final int COMPACTED_TOPIC_COMPACTION_LAG = 1000;
    private static final int DEFAULT_COMMIT_TIMEOUT = 60;
    private static final int ZK_SESSION_TIMEOUT = 30000;
    private static final int ZK_CONNECTION_TIMEOUT = 10000;
    private static final int ZK_MAX_IN_FLIGHT_REQUESTS = 1000;
    private static final int NAKADI_SEND_TIMEOUT = 10000;
    private static final int NAKADI_POLL_TIMEOUT = 10000;
    private static final Long DEFAULT_RETENTION_TIME = 100L;
    private static final Long DEFAULT_TOPIC_RETENTION = 100000000L;
    private static final CleanupPolicy DEFAULT_CLEANUP_POLICY = CleanupPolicy.DELETE;
    private static final int KAFKA_RETRIES = 10;
    private static final int KAFKA_REQUEST_TIMEOUT = 30000;
    private static final int KAFKA_DELIVERY_TIMEOUT = 30000;
    private static final int KAFKA_MAX_BLOCK_TIMEOUT = 5000;
    private static final int KAFKA_METADATA_MAX_AGE_MS = 1000;
    private static final String KAFKA_COMPRESSION_TYPE = "lz4";
    private static final int KAFKA_BATCH_SIZE = 1048576;
    private static final long KAFKA_BUFFER_MEMORY = KAFKA_BATCH_SIZE * 10L;
    private static final int KAFKA_LINGER_MS = 0;
    private static final int KAFKA_MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION = 5;
    private static final long NAKADI_EVENT_MAX_BYTES = 1000000L;
    private static final long TIMELINE_WAIT_TIMEOUT = 40000;
    private static final int NAKADI_SUBSCRIPTION_MAX_PARTITIONS = 8;
    private static final boolean KAFKA_ENABLE_AUTO_COMMIT = false;
    private static final int KAFKA_MAX_REQUEST_SIZE = 2098152;
    private static final String DEFAULT_ADMIN_DATA_TYPE = "service";
    private static final String DEFAULT_ADMIN_VALUE = "nakadi";
    private static final String DEFAULT_WARN_ALL_DATA_ACCESS_MESSAGE = "";
    private static final String DEFAULT_WARN_LOG_COMPACTION_MESSAGE = "";
    private static final String DEFAULT_EVENT_TYPE_DELETABLE_SUBSCRIPTION_OWNING_APPLICATION = "nakadi_archiver";
    private static final String DEFAULT_EVENT_TYPE_DELETABLE_SUBSCRIPTION_CONSUMER_GROUP = "nakadi_to_s3";
    private static final long DEFAULT_CURATOR_MAX_LIFETIME_MS = 1000;
    private static final long DEFAULT_CURATOR_ROTATION_MS = 10000;

    private NakadiSettings nakadiSettings;
    private KafkaSettings kafkaSettings;
    private KafkaTestHelper kafkaHelper;
    private KafkaTopicRepository kafkaTopicRepository;
    private NakadiTopicConfig defaultTopicConfig;
    private KafkaTopicConfigFactory kafkaTopicConfigFactory;

    @Before
    public void setup() {
        nakadiSettings = new NakadiSettings(
                MAX_TOPIC_PARTITION_COUNT,
                DEFAULT_PARTITION_COUNT,
                DEFAULT_REPLICA_FACTOR,
                DEFAULT_TOPIC_RETENTION,
                DEFAULT_TOPIC_ROTATION,
                DEFAULT_COMMIT_TIMEOUT,
                NAKADI_POLL_TIMEOUT,
                NAKADI_SEND_TIMEOUT,
                TIMELINE_WAIT_TIMEOUT,
                NAKADI_EVENT_MAX_BYTES,
                NAKADI_SUBSCRIPTION_MAX_PARTITIONS,
                DEFAULT_ADMIN_DATA_TYPE,
                DEFAULT_ADMIN_VALUE,
                DEFAULT_WARN_ALL_DATA_ACCESS_MESSAGE,
                DEFAULT_WARN_LOG_COMPACTION_MESSAGE,
                DEFAULT_EVENT_TYPE_DELETABLE_SUBSCRIPTION_OWNING_APPLICATION,
                DEFAULT_EVENT_TYPE_DELETABLE_SUBSCRIPTION_CONSUMER_GROUP,
                DEFAULT_CURATOR_MAX_LIFETIME_MS,
                DEFAULT_CURATOR_ROTATION_MS);

        kafkaSettings = new KafkaSettings(KAFKA_RETRIES, KAFKA_REQUEST_TIMEOUT, KAFKA_BATCH_SIZE, KAFKA_BUFFER_MEMORY,
                KAFKA_LINGER_MS, KAFKA_MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, KAFKA_ENABLE_AUTO_COMMIT,
                KAFKA_MAX_REQUEST_SIZE, KAFKA_DELIVERY_TIMEOUT, KAFKA_MAX_BLOCK_TIMEOUT, "", KAFKA_COMPRESSION_TYPE,
                KAFKA_METADATA_MAX_AGE_MS);
        kafkaHelper = new KafkaTestHelper(KAFKA_URL);
        defaultTopicConfig = new NakadiTopicConfig(DEFAULT_PARTITION_COUNT, DEFAULT_CLEANUP_POLICY,
                Optional.of(DEFAULT_RETENTION_TIME));
        kafkaTopicConfigFactory = new KafkaTopicConfigFactory(new UUIDGenerator(), DEFAULT_REPLICA_FACTOR,
                DEFAULT_TOPIC_ROTATION, COMPACTED_TOPIC_ROTATION, COMPACTED_TOPIC_SEGMENT_BYTES,
                COMPACTED_TOPIC_COMPACTION_LAG);
        kafkaTopicRepository = createKafkaTopicRepository();
    }

    @Test(timeout = 10000)
    @SuppressWarnings("unchecked")
    public void whenCreateTopicThenTopicIsCreated() {
        // ACT //
        final String topicName = kafkaTopicRepository.createTopic(defaultTopicConfig);

        // ASSERT //
        executeWithRetry(() -> {
                    final Map<String, List<PartitionInfo>> topics = getAllTopics();
                    assertThat(topics.keySet(), hasItem(topicName));

                    final List<PartitionInfo> partitionInfos = topics.get(topicName);
                    assertThat(partitionInfos, hasSize(DEFAULT_PARTITION_COUNT));

                    partitionInfos.forEach(pInfo ->
                            assertThat(pInfo.replicas(), arrayWithSize(DEFAULT_REPLICA_FACTOR)));

                    final Long retentionTime;
                    try {
                        retentionTime = KafkaTestHelper.getTopicRetentionTime(topicName);
                        assertThat(retentionTime, equalTo(DEFAULT_RETENTION_TIME));

                        final String cleanupPolicy = KafkaTestHelper.getTopicCleanupPolicy(topicName);
                        assertThat(cleanupPolicy, equalTo("delete"));

                        final String segmentMs = KafkaTestHelper.getTopicProperty(topicName, "segment.ms");
                        assertThat(segmentMs, equalTo(String.valueOf(DEFAULT_TOPIC_ROTATION)));
                    } catch (final Exception e) {
                        throw new RuntimeException(e);
                    }
                },
                new RetryForSpecifiedTimeStrategy<Void>(5000).withExceptionsThatForceRetry(AssertionError.class)
                        .withWaitBetweenEachTry(500));
    }

    @Test(timeout = 10000)
    @SuppressWarnings("unchecked")
    public void whenCreateCompactedTopicThenTopicIsCreated() {
        // ACT //
        final NakadiTopicConfig compactedTopicConfig = new NakadiTopicConfig(DEFAULT_PARTITION_COUNT,
                CleanupPolicy.COMPACT, Optional.empty());
        final String topicName = kafkaTopicRepository.createTopic(compactedTopicConfig);

        // ASSERT //
        executeWithRetry(() -> {
                    final Map<String, List<PartitionInfo>> topics = getAllTopics();
                    assertThat(topics.keySet(), hasItem(topicName));

                    final List<PartitionInfo> partitionInfos = topics.get(topicName);
                    assertThat(partitionInfos, hasSize(DEFAULT_PARTITION_COUNT));

                    partitionInfos.forEach(pInfo ->
                            assertThat(pInfo.replicas(), arrayWithSize(DEFAULT_REPLICA_FACTOR)));

                    try {
                        final String cleanupPolicy = KafkaTestHelper.getTopicCleanupPolicy(topicName);
                        assertThat(cleanupPolicy, equalTo("compact"));

                        final String segmentMs = KafkaTestHelper.getTopicProperty(topicName, "segment.ms");
                        assertThat(segmentMs, equalTo(String.valueOf(COMPACTED_TOPIC_ROTATION)));

                        final String segmentBytes = KafkaTestHelper.getTopicProperty(topicName,
                                "segment.bytes");
                        assertThat(segmentBytes, equalTo(String.valueOf(COMPACTED_TOPIC_SEGMENT_BYTES)));

                        final String compactionLag = KafkaTestHelper.getTopicProperty(topicName,
                                "min.compaction.lag.ms");
                        assertThat(compactionLag, equalTo(String.valueOf(COMPACTED_TOPIC_COMPACTION_LAG)));
                    } catch (final Exception e) {
                        throw new RuntimeException(e);
                    }
                },
                new RetryForSpecifiedTimeStrategy<Void>(5000).withExceptionsThatForceRetry(AssertionError.class)
                        .withWaitBetweenEachTry(500));
    }

    @Test(timeout = 20000)
    @SuppressWarnings("unchecked")
    public void whenDeleteTopicThenTopicIsDeleted() {

        // ARRANGE //
        final String topicName = UUID.randomUUID().toString();
        kafkaHelper.createTopic(topicName);

        // wait for topic to be created
        executeWithRetry(() -> {
                    return getAllTopics().containsKey(topicName);
                },
                new RetryForSpecifiedTimeStrategy<Boolean>(5000).withResultsThatForceRetry(false)
                        .withWaitBetweenEachTry(500));

        // ACT //
        kafkaTopicRepository.deleteTopic(topicName);

        // ASSERT //
        // check that topic was deleted
        executeWithRetry(() -> {
                    assertThat(getAllTopics().keySet(), Matchers.not(hasItem(topicName)));
                },
                new RetryForSpecifiedTimeStrategy<Void>(5000).withExceptionsThatForceRetry(AssertionError.class)
                        .withWaitBetweenEachTry(500));
    }

    @Test(timeout = 10000)
    public void whenBulkSendSuccessfullyThenUpdateBatchItemStatus() {
        final List<BatchItem> items = new ArrayList<>();
        final String topicId = TestUtils.randomValidEventTypeName();
        kafkaHelper.createTopic(topicId);

        for (int i = 0; i < 10; i++) {
            final BatchItem item = BatchFactory.from("[{}]").get(0);
            item.setPartition("0");
            items.add(item);
        }

        kafkaTopicRepository.syncPostBatch(topicId, items, null, false);

        for (int i = 0; i < 10; i++) {
            assertThat(items.get(i).getResponse().getPublishingStatus(), equalTo(EventPublishingStatus.SUBMITTED));
        }
    }

    @Test(timeout = 10000)
    public void whenSendBatchWithItemHeadersThenCheckBatchStatus() {
        final List<BatchItem> items = new ArrayList<>();
        final String topicId = TestUtils.randomValidEventTypeName();
        kafkaHelper.createTopic(topicId);

        for (int i = 0; i < 10; i++) {
            final BatchItem item = BatchFactory.from("[{}]").get(0);
            item.setPartition("0");
            item.setOwner(new EventOwnerHeader("unit", "Nakadi"));
            items.add(item);
        }
        kafkaTopicRepository.syncPostBatch(topicId, items, null, false);

        for (int i = 0; i < 10; i++) {
            assertThat(items.get(i).getResponse().getPublishingStatus(), equalTo(EventPublishingStatus.SUBMITTED));
        }
    }

    private Map<String, List<PartitionInfo>> getAllTopics() {
        final KafkaConsumer<byte[], byte[]> kafkaConsumer = kafkaHelper.createConsumer();
        return kafkaConsumer.listTopics();
    }

    private KafkaTopicRepository createKafkaTopicRepository() {
        final KafkaZookeeper kafkaZookeeper = Mockito.mock(KafkaZookeeper.class);
        Mockito.when(kafkaZookeeper.getZookeeperConnectionString()).thenReturn(ZOOKEEPER_URL);

        final Consumer<byte[], byte[]> consumer = Mockito.mock(Consumer.class);
        Mockito.when(consumer.partitionsFor(any())).thenReturn(new ArrayList<>());

        final KafkaFactory factory = Mockito.mock(KafkaFactory.class);
        Mockito.when(factory.getConsumer()).thenReturn(consumer);
        final KafkaLocationManager kafkaLocationManager = Mockito.mock(KafkaLocationManager.class);
        Mockito
                .when(kafkaLocationManager.getProperties())
                .thenReturn(createKafkaProperties());

        Mockito
                .doReturn(kafkaHelper.createProducer())
                .when(factory)
                .takeDefaultProducer();
        Mockito
                .doReturn(kafkaHelper.createProducer())
                .when(factory)
                .takeProducer(anyString());

        return new KafkaTopicRepository.Builder()
                .setKafkaZookeeper(kafkaZookeeper)
                .setKafkaFactory(factory)
                .setNakadiSettings(nakadiSettings)
                .setKafkaSettings(kafkaSettings)
                .setKafkaTopicConfigFactory(kafkaTopicConfigFactory)
                .setKafkaLocationManager(kafkaLocationManager)
                .build();
    }

}
