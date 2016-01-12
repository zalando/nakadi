package de.zalando.aruha.nakadi.repository.zookeeper;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.io.IOException;

public class ZooKeeperHolder {

    private static final Logger LOG = LoggerFactory.getLogger(ZooKeeperHolder.class);

    private final String brokers;

    private ZooKeeper zooKeeper;

    public ZooKeeperHolder(final String brokers) {
        this.brokers = brokers;
    }

    @PostConstruct
    public void init() throws IOException {
        LOG.info("Start ZooKeeper client for brokers: {}", brokers);
        zooKeeper = new ZooKeeper(brokers, 30000, new Watcher() {
            @Override
            public void process(final WatchedEvent event) {
                LOG.info("connection state event: {}", event);
            }
        });
    }

    public ZooKeeper get() throws IOException {
        return zooKeeper;
    }
}
