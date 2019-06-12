package org.zalando.nakadi.repository.zookeeper;

import org.apache.curator.framework.recipes.locks.InterProcessLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Callable;

public class ZookeeperUtils {

    private static final Logger LOG = LoggerFactory.getLogger(ZookeeperUtils.class);

    private ZookeeperUtils() {
    }

    public static <V> V runLocked(final Callable<V> callable, final InterProcessLock lock) throws Exception {
        lock.acquire();
        try {
            return callable.call();
        } finally {
            try {
                lock.release();
            } catch (final Exception e) {
                LOG.warn("Error occurred when releasing ZK lock", e);
            }
        }
    }
}
