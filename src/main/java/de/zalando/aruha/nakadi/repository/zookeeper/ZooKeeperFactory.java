package de.zalando.aruha.nakadi.repository.zookeeper;

import java.io.IOException;

import org.apache.zookeeper.ZooKeeper;

import org.springframework.beans.factory.annotation.Value;

import org.springframework.context.annotation.PropertySource;

import org.springframework.stereotype.Component;

@Component
@PropertySource("${nakadi.config}")
public class ZooKeeperFactory {
    @Value("${nakadi.zookeeper.brokers}")
    private String brokers;

    public ZooKeeper get() throws IOException {
        return new ZooKeeper(brokers, 30000, null);
    }

}
