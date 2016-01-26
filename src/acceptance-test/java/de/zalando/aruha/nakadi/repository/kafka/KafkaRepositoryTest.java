package de.zalando.aruha.nakadi.repository.kafka;

import de.zalando.aruha.nakadi.repository.KafkaRepositorySettings;
import de.zalando.aruha.nakadi.repository.zookeeper.ZooKeeperHolder;
import de.zalando.aruha.nakadi.utils.TestUtils;
import org.apache.curator.CuratorZookeeperClient;
import org.apache.curator.framework.CuratorFramework;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.junit.Before;
import org.junit.Test;

import java.util.List;
import java.util.Map;
import java.util.Properties;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.arrayWithSize;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasSize;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class KafkaRepositoryTest {

    private static final String zookeeperURL = "localhost:2181";
    private static final String kafkaURL = "localhost:9092";
    private static final int defaultPartitionNum = 8;
    private static final int defaultReplicaFactor = 1;

    private KafkaRepositorySettings repositorySettings;
    private Properties kafkaProperties;

    @Before
    public void setup() {
        repositorySettings = createRepositorySettings();
        kafkaProperties = createKafkaProperties();
    }

    @Test(timeout = 10000)
    public void whenCreateTopicThenTopicIsCreated() throws InterruptedException {

        // ARRANGE //
        final String topicName = TestUtils.randomString();

        final CuratorZookeeperClient zookeeperClient = mock(CuratorZookeeperClient.class);
        when(zookeeperClient.getCurrentConnectionString()).thenReturn(zookeeperURL);

        final CuratorFramework curatorFramework = mock(CuratorFramework.class);
        when(curatorFramework.getZookeeperClient()).thenReturn(zookeeperClient);

        final ZooKeeperHolder zooKeeperHolder = mock(ZooKeeperHolder.class);
        when(zooKeeperHolder.get()).thenReturn(curatorFramework);

        final KafkaRepository kafkaRepository = new KafkaRepository(zooKeeperHolder, mock(KafkaFactory.class),
                repositorySettings);

        // ACT //
        kafkaRepository.createTopic(topicName);

        // ASSERT //
        Thread.sleep(2000);

        final KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(kafkaProperties);
        final Map<String, List<PartitionInfo>> topics = kafkaConsumer.listTopics();

        assertThat(topics.keySet(), hasItem(topicName));

        final List<PartitionInfo> partitionInfos = topics.get(topicName);
        assertThat(partitionInfos, hasSize(defaultPartitionNum));

        partitionInfos
                .stream()
                .forEach(pInfo -> assertThat(pInfo.replicas(), arrayWithSize(defaultReplicaFactor)));
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

    private Properties createKafkaProperties() {
        final Properties props = new Properties();
        props.put("bootstrap.servers", kafkaURL);
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        return props;
    }
}
