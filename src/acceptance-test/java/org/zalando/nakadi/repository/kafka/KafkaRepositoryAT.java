package org.zalando.nakadi.repository.kafka;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.curator.CuratorZookeeperClient;
import org.apache.curator.framework.CuratorFramework;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.echocat.jomon.runtime.concurrent.RetryForSpecifiedTimeStrategy;
import static org.echocat.jomon.runtime.concurrent.Retryer.executeWithRetry;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.arrayWithSize;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.not;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import static org.mockito.Matchers.any;
import org.mockito.Mockito;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import org.zalando.nakadi.config.NakadiSettings;
import org.zalando.nakadi.domain.BatchItem;
import org.zalando.nakadi.domain.EventPublishingStatus;
import org.zalando.nakadi.repository.zookeeper.ZooKeeperHolder;
import org.zalando.nakadi.repository.zookeeper.ZookeeperSettings;
import org.zalando.nakadi.util.UUIDGenerator;
import org.zalando.nakadi.utils.TestUtils;
import org.zalando.nakadi.webservice.BaseAT;

public class KafkaRepositoryAT extends BaseAT {

    private static final int DEFAULT_PARTITION_COUNT = 8;
    private static final int DEFAULT_REPLICA_FACTOR = 1;
    private static final int MAX_TOPIC_PARTITION_COUNT = 10;
    private static final int DEFAULT_TOPIC_ROTATION = 50000000;
    private static final int DEFAULT_COMMIT_TIMEOUT = 60;
    private static final int ZK_SESSION_TIMEOUT = 30000;
    private static final int ZK_CONNECTION_TIMEOUT = 10000;
    private static final int NAKADI_SEND_TIMEOUT = 10000;
    private static final int NAKADI_POLL_TIMEOUT = 10000;
    private static final Long RETENTION_TIME = 100L;
    private static final Long DEFAULT_TOPIC_RETENTION = 100000000L;
    private static final int KAFKA_REQUEST_TIMEOUT = 30000;
    private static final int KAFKA_BATCH_SIZE = 1048576;
    private static final long KAFKA_LINGER_MS = 0;
    private static final long NAKADI_EVENT_MAX_BYTES = 1000000L;
    private static final long TIMELINE_WAIT_TIMEOUT = 40000;
    private static final int NAKADI_SUBSCRIPTION_MAX_PARTITIONS = 8;
    private static final boolean KAFKA_ENABLE_AUTO_COMMIT = false;

    private NakadiSettings nakadiSettings;
    private KafkaSettings kafkaSettings;
    private ZookeeperSettings zookeeperSettings;
    private KafkaTestHelper kafkaHelper;
    private KafkaTopicRepository kafkaTopicRepository;

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
                NAKADI_SUBSCRIPTION_MAX_PARTITIONS);
        kafkaSettings = new KafkaSettings(KAFKA_REQUEST_TIMEOUT, KAFKA_BATCH_SIZE,
                KAFKA_LINGER_MS, KAFKA_ENABLE_AUTO_COMMIT);
        zookeeperSettings = new ZookeeperSettings(ZK_SESSION_TIMEOUT, ZK_CONNECTION_TIMEOUT);
        kafkaHelper = new KafkaTestHelper(KAFKA_URL);
        kafkaTopicRepository = createKafkaTopicRepository();
    }

    @Test(timeout = 10000)
    @SuppressWarnings("unchecked")
    public void whenCreateTopicThenTopicIsCreated() throws Exception {
        // ACT //
        final String topicName = kafkaTopicRepository.createTopic(DEFAULT_PARTITION_COUNT, RETENTION_TIME);

        // ASSERT //
        executeWithRetry(() -> {
                    final Map<String, List<PartitionInfo>> topics = getAllTopics();
                    assertThat(topics.keySet(), hasItem(topicName));

                    final List<PartitionInfo> partitionInfos = topics.get(topicName);
                    assertThat(partitionInfos, hasSize(DEFAULT_PARTITION_COUNT));

                    partitionInfos.forEach(pInfo ->
                            assertThat(pInfo.replicas(), arrayWithSize(DEFAULT_REPLICA_FACTOR)));
                },
                new RetryForSpecifiedTimeStrategy<Void>(5000).withExceptionsThatForceRetry(AssertionError.class)
                        .withWaitBetweenEachTry(500));
    }

    @Test(timeout = 20000)
    @SuppressWarnings("unchecked")
    public void whenDeleteTopicThenTopicIsDeleted() throws Exception {

        // ARRANGE //
        final String topicName = UUID.randomUUID().toString();
        kafkaHelper.createTopic(topicName, ZOOKEEPER_URL);

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
                    assertThat(getAllTopics().keySet(), not(hasItem(topicName)));
                },
                new RetryForSpecifiedTimeStrategy<Void>(5000).withExceptionsThatForceRetry(AssertionError.class)
                        .withWaitBetweenEachTry(500));
    }

    @Test(timeout = 10000)
    public void whenBulkSendSuccessfullyThenUpdateBatchItemStatus() throws Exception {
        final List<BatchItem> items = new ArrayList<>();
        final String event = "{}";
        final String topicId = TestUtils.randomValidEventTypeName();
        kafkaHelper.createTopic(topicId, ZOOKEEPER_URL);

        for (int i = 0; i < 10; i++) {
            final BatchItem item = new BatchItem(event);
            item.setPartition("0");
            items.add(item);
        }

        kafkaTopicRepository.syncPostBatch(topicId, items);

        for (int i = 0; i < 10; i++) {
            assertThat(items.get(i).getResponse().getPublishingStatus(), equalTo(EventPublishingStatus.SUBMITTED));
        }
    }

    @Test(timeout = 10000)
    @SuppressWarnings("unchecked")
    public void whenCreateTopicWithRetentionTime() throws Exception {
        // ACT //
        final String topicName = kafkaTopicRepository.createTopic(DEFAULT_PARTITION_COUNT, RETENTION_TIME);

        // ASSERT //
        executeWithRetry(() -> Assert.assertEquals(
                KafkaTestHelper.getTopicRetentionTime(topicName, ZOOKEEPER_URL), RETENTION_TIME),
                new RetryForSpecifiedTimeStrategy<Void>(5000)
                        .withExceptionsThatForceRetry(AssertionError.class)
                        .withWaitBetweenEachTry(500));
    }

    private Map<String, List<PartitionInfo>> getAllTopics() {
        final KafkaConsumer<String, String> kafkaConsumer = kafkaHelper.createConsumer();
        return kafkaConsumer.listTopics();
    }

    private KafkaTopicRepository createKafkaTopicRepository() {
        final CuratorZookeeperClient zookeeperClient = mock(CuratorZookeeperClient.class);
        when(zookeeperClient.getCurrentConnectionString()).thenReturn(ZOOKEEPER_URL);

        final CuratorFramework curatorFramework = mock(CuratorFramework.class);
        when(curatorFramework.getZookeeperClient()).thenReturn(zookeeperClient);

        final ZooKeeperHolder zooKeeperHolder = mock(ZooKeeperHolder.class);
        when(zooKeeperHolder.get()).thenReturn(curatorFramework);

        final Consumer<byte[], byte[]> consumer = mock(Consumer.class);
        when(consumer.partitionsFor(any())).thenReturn(new ArrayList<>());

        final KafkaFactory factory = mock(KafkaFactory.class);
        when(factory.getConsumer()).thenReturn(consumer);

        Mockito
                .doReturn(kafkaHelper.createProducer())
                .when(factory)
                .takeProducer();

        return new KafkaTopicRepository(zooKeeperHolder,
                factory,
                nakadiSettings,
                kafkaSettings,
                zookeeperSettings,
                new UUIDGenerator());
    }

}
