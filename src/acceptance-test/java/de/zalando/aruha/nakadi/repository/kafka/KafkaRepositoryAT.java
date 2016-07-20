package de.zalando.aruha.nakadi.repository.kafka;

import de.zalando.aruha.nakadi.domain.BatchItem;
import de.zalando.aruha.nakadi.domain.EventPublishingStatus;
import de.zalando.aruha.nakadi.domain.EventType;
import de.zalando.aruha.nakadi.domain.EventTypeOptions;
import de.zalando.aruha.nakadi.repository.zookeeper.ZooKeeperHolder;
import de.zalando.aruha.nakadi.utils.TestUtils;
import de.zalando.aruha.nakadi.webservice.BaseAT;
import kafka.admin.AdminUtils;
import kafka.server.ConfigType;
import kafka.utils.ZkUtils;
import org.apache.curator.CuratorZookeeperClient;
import org.apache.curator.framework.CuratorFramework;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.echocat.jomon.runtime.concurrent.RetryForSpecifiedTimeStrategy;
import org.json.JSONObject;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static org.echocat.jomon.runtime.concurrent.Retryer.executeWithRetry;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.arrayWithSize;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.not;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class KafkaRepositoryAT extends BaseAT {

    private static final int DEFAULT_PARTITION_COUNT = 8;
    private static final int DEFAULT_REPLICA_FACTOR = 1;
    private static final Long RETENTION_TIME = 100L;
    private static final Long DEFAULT_TOPIC_RETENTION_MS = 100000000L;

    private KafkaRepositorySettings repositorySettings;
    private KafkaTestHelper kafkaHelper;
    private KafkaTopicRepository kafkaTopicRepository;
    private String topicName;
    private String eventName;

    @Before
    public void setup() {
        repositorySettings = createRepositorySettings();
        kafkaHelper = new KafkaTestHelper(KAFKA_URL);
        kafkaTopicRepository = createKafkaTopicRepository();
        topicName = TestUtils.randomUUID();
        eventName = TestUtils.randomValidEventTypeName();
    }

    @Test(timeout = 10000)
    @SuppressWarnings("unchecked")
    public void whenCreateTopicThenTopicIsCreated() throws Exception {
        final EventType eventType = new EventType();
        eventType.setName(eventName);
        eventType.setTopic(topicName);
        // ACT //
        kafkaTopicRepository.createTopic(eventType);

        // ASSERT //
        executeWithRetry(() -> {
                final Map<String, List<PartitionInfo>> topics = getAllTopics();
                assertThat(topics.keySet(), hasItem(topicName));

                final List<PartitionInfo> partitionInfos = topics.get(topicName);
                assertThat(partitionInfos, hasSize(DEFAULT_PARTITION_COUNT));

                partitionInfos.stream().forEach(pInfo ->
                        assertThat(pInfo.replicas(), arrayWithSize(DEFAULT_REPLICA_FACTOR)));
            },
            new RetryForSpecifiedTimeStrategy<Void>(5000).withExceptionsThatForceRetry(AssertionError.class)
                .withWaitBetweenEachTry(500));
    }

    @Test(timeout = 20000)
    @SuppressWarnings("unchecked")
    public void whenDeleteTopicThenTopicIsDeleted() throws Exception {

        // ARRANGE //
        kafkaHelper.createTopic(topicName, ZOOKEEPER_URL);

        // wait for topic to be created
        executeWithRetry(() -> { return getAllTopics().containsKey(topicName); },
            new RetryForSpecifiedTimeStrategy<Boolean>(5000).withResultsThatForceRetry(false).withWaitBetweenEachTry(
                500));

        // ACT //
        kafkaTopicRepository.deleteTopic(topicName);

        // ASSERT //
        // check that topic was deleted
        executeWithRetry(() -> { assertThat(getAllTopics().keySet(), not(hasItem(topicName))); },
            new RetryForSpecifiedTimeStrategy<Void>(5000).withExceptionsThatForceRetry(AssertionError.class)
                    .withWaitBetweenEachTry(500));
    }

    @Test(timeout = 10000)
    public void whenBulkSendSuccessfullyThenUpdateBatchItemStatus() throws Exception {
        final List<BatchItem> items = new ArrayList<>();
        final JSONObject event = new JSONObject();
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
    public void whenCreateTopicWithRetentionTime() throws Exception {
        final EventType eventType = new EventType();
        eventType.setName(eventName);
        eventType.setTopic(topicName);
        final EventTypeOptions options = new EventTypeOptions();
        options.setRetentionTime(RETENTION_TIME);
        eventType.setOptions(options);
        // ACT //
        kafkaTopicRepository.createTopic(eventType);


        // ASSERT //
        executeWithRetry(() -> Assert.assertEquals(getTopicRetentionTime(topicName), RETENTION_TIME),
                new RetryForSpecifiedTimeStrategy<Void>(5000).withExceptionsThatForceRetry(AssertionError.class)
                        .withWaitBetweenEachTry(500));
    }

    @Test(timeout = 10000)
    public void whenCreateTopicWithDefaultRetentionTime() throws Exception {
        final EventType eventType = new EventType();
        eventType.setName(eventName);
        eventType.setTopic(topicName);
        // ACT //
        kafkaTopicRepository.createTopic(eventType);


        // ASSERT //
        executeWithRetry(() -> Assert.assertEquals(getTopicRetentionTime(topicName), DEFAULT_TOPIC_RETENTION_MS),
                new RetryForSpecifiedTimeStrategy<Void>(5000).withExceptionsThatForceRetry(AssertionError.class)
                        .withWaitBetweenEachTry(500));
    }

    private Long getTopicRetentionTime(final String topic) {
        final ZkUtils zkUtils = ZkUtils.apply(ZOOKEEPER_URL, 30000, 10000, false);
        final Properties topicConfig = AdminUtils.fetchEntityConfig(zkUtils, ConfigType.Topic(), topic);
        return Long.valueOf(topicConfig.getProperty("retention.ms"));
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

        final KafkaFactory factory = mock(KafkaFactory.class);

        Mockito
                .doReturn(kafkaHelper.createProducer())
                .when(factory)
                .getProducer();

        return new KafkaTopicRepository(zooKeeperHolder, factory, repositorySettings, null);
    }

    private KafkaRepositorySettings createRepositorySettings() {
        final KafkaRepositorySettings settings = new KafkaRepositorySettings();
        settings.setDefaultTopicPartitionCount(DEFAULT_PARTITION_COUNT);
        settings.setDefaultTopicReplicaFactor(DEFAULT_REPLICA_FACTOR);
        settings.setKafkaSendTimeoutMs(10000);
        settings.setDefaultTopicRetentionMs(DEFAULT_TOPIC_RETENTION_MS);
        settings.setDefaultTopicRotationMs(50000000);
        settings.setZkSessionTimeoutMs(30000);
        settings.setZkConnectionTimeoutMs(10000);
        return settings;
    }

}
