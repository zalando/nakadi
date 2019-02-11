package org.zalando.nakadi.repository.zookeeper;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class ZookeeperConfig {

    private final int zkSessionTimeoutMs;
    private final int zkConnectionTimeoutMs;
    private final String zookeeperConn;

    public ZookeeperConfig(
            @Value("${nakadi.zookeeper.conn}") final String zookeeperConn,
            @Value("${nakadi.zookeeper.sessionTimeoutMs}") final int zkSessionTimeoutMs,
            @Value("${nakadi.zookeeper.connectionTimeoutMs}") final int zkConnectionTimeoutMs) {
        this.zookeeperConn = zookeeperConn;
        this.zkSessionTimeoutMs = zkSessionTimeoutMs;
        this.zkConnectionTimeoutMs = zkConnectionTimeoutMs;
    }

    public int getZkSessionTimeoutMs() {
        return zkSessionTimeoutMs;
    }

    public int getZkConnectionTimeoutMs() {
        return zkConnectionTimeoutMs;
    }

    public String getZookeeperConn() {
        return zookeeperConn;
    }

    @Bean
    public ZooKeeperHolder zooKeeperHolder() {
        return ZooKeeperHolder.build(zookeeperConn);
    }

}
