package org.zalando.nakadi.repository.zookeeper;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

@Component
public class ZookeeperSettings {

    private final int zkSessionTimeoutMs;
    private final int zkConnectionTimeoutMs;

    @Autowired
    public ZookeeperSettings(@Value("${nakadi.zookeeper.sessionTimeoutMs}") final int zkSessionTimeoutMs,
                             @Value("${nakadi.zookeeper.connectionTimeoutMs}")final int zkConnectionTimeoutMs) {
        this.zkSessionTimeoutMs = zkSessionTimeoutMs;
        this.zkConnectionTimeoutMs = zkConnectionTimeoutMs;
    }

    public int getZkSessionTimeoutMs() {
        return zkSessionTimeoutMs;
    }

    public int getZkConnectionTimeoutMs() {
        return zkConnectionTimeoutMs;
    }
}
