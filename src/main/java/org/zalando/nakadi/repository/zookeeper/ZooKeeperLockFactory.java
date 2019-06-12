package org.zalando.nakadi.repository.zookeeper;

import org.apache.curator.framework.recipes.locks.InterProcessLock;
import org.apache.curator.framework.recipes.locks.InterProcessSemaphoreMutex;

public class ZooKeeperLockFactory {

    private final ZooKeeperHolder zkHolder;

    public ZooKeeperLockFactory(final ZooKeeperHolder zkHolder) {
        this.zkHolder = zkHolder;
    }

    public InterProcessLock createLock(final String path) {
        return new InterProcessSemaphoreMutex(zkHolder.get(), path);
    }
}
