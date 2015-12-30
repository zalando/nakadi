package de.zalando.aruha.nakadi.repository.zookeeper;

import java.io.IOException;

import javax.annotation.PostConstruct;

import org.apache.zookeeper.ZooKeeper;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;

import org.springframework.stereotype.Component;

@Component
public class ZooKeeperHolder {

    @Autowired
    @Qualifier("zookeeperBrokers")
    private String brokers;

    private ZooKeeper zooKeeper;

    @PostConstruct
    public void init() throws IOException {
        zooKeeper = new ZooKeeper(brokers, 30000, null);
    }

    public ZooKeeper get() throws IOException {
        return zooKeeper;
    }
}
