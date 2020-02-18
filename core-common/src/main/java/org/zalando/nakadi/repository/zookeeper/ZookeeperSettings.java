package org.zalando.nakadi.repository.zookeeper;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

@Component
public class ZookeeperSettings {

    private final int zkSessionTimeoutMs;
    private final int zkConnectionTimeoutMs;
    private final int maxInFlightRequests;
    public static final String METRIC_GROUP = "kafka.server";
    public static final String METRIC_TYPE = "kakfaZookeeper";


    @Autowired
    public ZookeeperSettings(@Value("${nakadi.zookeeper.sessionTimeoutMs}") final int zkSessionTimeoutMs,
                             @Value("${nakadi.zookeeper.connectionTimeoutMs}")final int zkConnectionTimeoutMs,
                             @Value("${nakadi.zookeeper.maxInFlightRequests}") final int maxInFlightRequests) {
        this.zkSessionTimeoutMs = zkSessionTimeoutMs;
        this.zkConnectionTimeoutMs = zkConnectionTimeoutMs;
        this.maxInFlightRequests = maxInFlightRequests;
    }

    public int getZkSessionTimeoutMs() {
        return zkSessionTimeoutMs;
    }

    public int getZkConnectionTimeoutMs() {
        return zkConnectionTimeoutMs;
    }

    public int getMaxInFlightRequests() {
        return maxInFlightRequests;
    }
}
