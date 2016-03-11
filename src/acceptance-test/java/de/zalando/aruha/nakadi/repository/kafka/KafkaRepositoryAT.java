package de.zalando.aruha.nakadi.repository.kafka;

import static org.echocat.jomon.runtime.concurrent.Retryer.executeWithRetry;

import static org.hamcrest.MatcherAssert.assertThat;

import static org.hamcrest.Matchers.arrayWithSize;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.not;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.Map;

import org.apache.curator.CuratorZookeeperClient;
import org.apache.curator.framework.CuratorFramework;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;

import org.echocat.jomon.runtime.concurrent.RetryForSpecifiedTimeStrategy;

import org.junit.Before;
import org.junit.Test;

import de.zalando.aruha.nakadi.repository.zookeeper.ZooKeeperHolder;
import de.zalando.aruha.nakadi.utils.TestUtils;
import de.zalando.aruha.nakadi.webservice.BaseAT;

public class KafkaRepositoryAT extends BaseAT {

    private static final int defaultPartitionNum = 8;
    private static final int defaultReplicaFactor = 1;

    private KafkaRepositorySettings repositorySettings;
    private KafkaTestHelper kafkaHelper;
    private KafkaTopicRepository kafkaTopicRepository;
    private String topicName;

    @Before
    public void setup() {
        repositorySettings = createRepositorySettings();
        kafkaHelper = new KafkaTestHelper(kafkaUrl);
        kafkaTopicRepository = createKafkaTopicRepository();
        topicName = TestUtils.randomValidEventTypeName();
    }

    @Test(timeout = 10000)
    @SuppressWarnings("unchecked")
    public void whenCreateTopicThenTopicIsCreated() throws Exception {

        // ACT //
        kafkaTopicRepository.createTopic(topicName);

        // ASSERT //
        executeWithRetry(() -> {
                final Map<String, List<PartitionInfo>> topics = getAllTopics();
                assertThat(topics.keySet(), hasItem(topicName));

                final List<PartitionInfo> partitionInfos = topics.get(topicName);
                assertThat(partitionInfos, hasSize(defaultPartitionNum));

                partitionInfos.stream().forEach(pInfo ->
                        assertThat(pInfo.replicas(), arrayWithSize(defaultReplicaFactor)));
            },
            new RetryForSpecifiedTimeStrategy<Void>(5000).withExceptionsThatForceRetry(AssertionError.class)
                .withWaitBetweenEachTry(500));
    }

    @Test(timeout = 20000)
    @SuppressWarnings("unchecked")
    public void whenDeleteTopicThenTopicIsDeleted() throws Exception {

        // ARRANGE //
        kafkaHelper.createTopic(topicName, zookeeperUrl);

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

    private Map<String, List<PartitionInfo>> getAllTopics() {
        final KafkaConsumer<String, String> kafkaConsumer = kafkaHelper.createConsumer();
        return kafkaConsumer.listTopics();
    }

    private KafkaTopicRepository createKafkaTopicRepository() {
        final CuratorZookeeperClient zookeeperClient = mock(CuratorZookeeperClient.class);
        when(zookeeperClient.getCurrentConnectionString()).thenReturn(zookeeperUrl);

        final CuratorFramework curatorFramework = mock(CuratorFramework.class);
        when(curatorFramework.getZookeeperClient()).thenReturn(zookeeperClient);

        final ZooKeeperHolder zooKeeperHolder = mock(ZooKeeperHolder.class);
        when(zooKeeperHolder.get()).thenReturn(curatorFramework);

        return new KafkaTopicRepository(zooKeeperHolder, mock(KafkaFactory.class), repositorySettings);
    }

    private KafkaRepositorySettings createRepositorySettings() {
        final KafkaRepositorySettings settings = new KafkaRepositorySettings();
        settings.setDefaultTopicPartitionNum(defaultPartitionNum);
        settings.setDefaultTopicReplicaFactor(defaultReplicaFactor);
        settings.setDefaultTopicRetentionMs(100000000);
        settings.setDefaultTopicRotationMs(50000000);
        settings.setZkSessionTimeoutMs(30000);
        settings.setZkConnectionTimeoutMs(10000);
        return settings;
    }

}
