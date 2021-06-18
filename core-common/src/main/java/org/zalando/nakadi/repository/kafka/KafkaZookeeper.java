package org.zalando.nakadi.repository.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.zalando.nakadi.repository.zookeeper.ZooKeeperHolder;

import java.util.List;

public class KafkaZookeeper {
    private final ZooKeeperHolder zooKeeperHolder;
    private final ObjectMapper objectMapper;

    public KafkaZookeeper(
            final ZooKeeperHolder zooKeeperHolder,
            final ObjectMapper objectMapper) {
        this.zooKeeperHolder = zooKeeperHolder;
        this.objectMapper = objectMapper;
    }

    public List<String> listTopics() throws Exception {
        return zooKeeperHolder.get()
                .getChildren()
                .forPath("/brokers/topics");
    }

    public List<String> getBrokerIdsForSizeStats() throws Exception {
        return zooKeeperHolder.get().getChildren()
                .forPath("/bubuku/size_stats");
    }

    public BubukuSizeStats getSizeStatsForBroker(final String brokerId) throws Exception {
        return objectMapper.readValue(
                zooKeeperHolder.get().getData().forPath("/bubuku/size_stats/" + brokerId),
                BubukuSizeStats.class);
    }

    public String getZookeeperConnectionString() {
        return zooKeeperHolder.get().getZookeeperClient().getCurrentConnectionString();
    }
}
