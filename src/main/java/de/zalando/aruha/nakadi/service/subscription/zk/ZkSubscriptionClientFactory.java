package de.zalando.aruha.nakadi.service.subscription.zk;

import de.zalando.aruha.nakadi.repository.zookeeper.ZooKeeperHolder;

public class ZkSubscriptionClientFactory {

    private final ZooKeeperHolder zkHolder;

    public ZkSubscriptionClientFactory(final ZooKeeperHolder zkHolder) {
        this.zkHolder = zkHolder;
    }

    public ZkSubscriptionClient createZkSubscriptionClient(final String subscriptionId) {
        return new CuratorZkSubscriptionClient(subscriptionId, zkHolder.get());
    }
}
