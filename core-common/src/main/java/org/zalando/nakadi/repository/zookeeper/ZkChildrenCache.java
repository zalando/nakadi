package org.zalando.nakadi.repository.zookeeper;

import com.google.common.collect.ImmutableList;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.zookeeper.KeeperException;
import org.echocat.jomon.runtime.concurrent.RetryForSpecifiedCountStrategy;
import org.echocat.jomon.runtime.concurrent.Retryer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zalando.nakadi.exceptions.runtime.NakadiRuntimeException;

public class ZkChildrenCache extends PathChildrenCache {

    private static final Logger LOG = LoggerFactory.getLogger(ZkChildrenCache.class);

    public static final int MAX_NUMBER_OF_RETRIES = 5;
    public static final int WAIT_BETWEEN_TRIES_MS = 100;

    public ZkChildrenCache(final CuratorFramework client, final String path) {
        super(client, path, false);
    }

    @Override
    public void start() throws Exception {
        try {
            super.start(StartMode.BUILD_INITIAL_CACHE);
        } catch (final Exception e) {
            close();
            throw e;
        }
    }

    public static ZkChildrenCache createCache(final CuratorFramework client, final String key) {
        try {
            // in some rare case the cache start can fail because the node can be removed
            // in specific moment by other thread/instance, then we need to retry
            return Retryer.executeWithRetry(
                    () -> {
                        final ZkChildrenCache newCache = new ZkChildrenCache(client, key);
                        try {
                            newCache.start();
                            return newCache;
                        } catch (final KeeperException.NoNodeException e) {
                            throw e; // throw it to activate retry
                        } catch (final Exception e) {
                            throw new NakadiRuntimeException(e);
                        }
                    },
                    new RetryForSpecifiedCountStrategy<ZkChildrenCache>(MAX_NUMBER_OF_RETRIES)
                            .withExceptionsThatForceRetry(ImmutableList.of(KeeperException.NoNodeException.class))
                            .withWaitBetweenEachTry(WAIT_BETWEEN_TRIES_MS));
        } catch (final Exception e) {
            LOG.error("Zookeeper error when creating cache for children", e);
            throw new NakadiRuntimeException(e);
        }
    }
}
