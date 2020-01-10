package org.zalando.nakadi.cache;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zalando.nakadi.exceptions.runtime.NakadiRuntimeException;
import org.zalando.nakadi.repository.zookeeper.ZooKeeperHolder;
import org.zalando.nakadi.util.ThreadUtils;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * ZookeeperNodeInvalidator keeps track of zookeeper node and reacts on any kind of changes made to it.
 * In case if change actually occurred - the node invalidator will invalidate whole cache, waiting for it to be
 * refreshed partially, according to currently cached values.
 */
public class ZookeeperNodeInvalidator {
    private final Cache<?> cache;
    private final ZooKeeperHolder zooKeeperHolder;
    private final String cachePath;
    private final long forceRefreshMs;

    private Thread updatesThread;
    private long lastZxId;
    private boolean enforceWatcherCreation;

    private final Lock updateLock = new ReentrantLock();
    private final Condition refreshZookeeper = updateLock.newCondition();

    private static final long CALM_DOWN_PERIOD_MS = TimeUnit.SECONDS.toMillis(5);
    private static final Logger LOG = LoggerFactory.getLogger(ZookeeperNodeInvalidator.class);
    private static final AtomicInteger THREAD_COUNTER = new AtomicInteger(0);

    public ZookeeperNodeInvalidator(
            final Cache<?> cache,
            final ZooKeeperHolder zooKeeperHolder,
            final String cachePath,
            final long forceRefreshMs) {
        this.cache = cache;
        this.zooKeeperHolder = zooKeeperHolder;
        this.cachePath = cachePath;
        this.forceRefreshMs = forceRefreshMs;
    }

    /**
     * Starts thread with zookeeper listener that is responsible for cache updates.
     */
    public synchronized void start() {
        if (null != updatesThread) {
            throw new IllegalStateException("Can't start update thread twice");
        }
        updatesThread = new Thread(
                this::doPeriodicUpdate,
                "periodic-cache-updates-" + THREAD_COUNTER.incrementAndGet());
        updatesThread.start();
        this.zooKeeperHolder.get().getConnectionStateListenable().addListener(this::connectionStateChanged);
    }

    /**
     * Stops zookeper listener and thread responsible for cache updates.
     */
    public synchronized void stop() {
        if (null == updatesThread) {
            return;
        }
        updatesThread.interrupt();
        try {
            updatesThread.join();
        } catch (final InterruptedException ex) {
            LOG.error("Failed to wait for updates thread to stop", ex);
            Thread.currentThread().interrupt();
        } finally {
            updatesThread = null;
        }
    }

    /**
     * Provides information to all the nakadi instances about the fact that cache should be invalidated.
     */
    public void notifyUpdate() {
        for (int attempt = 0; attempt < 10; ++attempt) {
            try {
                zooKeeperHolder.get().setData().forPath(this.cachePath);
                return;
            } catch (InterruptedException ex) {
                Thread.currentThread().interrupt();
                throw new NakadiRuntimeException(ex);
            } catch (Exception e) {
                LOG.error("Failed to notify about cache refresh", e);
            }
        }
        throw new RuntimeException("Failed to update cache (see log before with stack trace of the problem)");
    }

    private static class UpdateRequirement {
        private final boolean needUpdate;
        private final boolean needRetry;

        UpdateRequirement(final boolean needUpdate, final boolean needRetry) {
            this.needUpdate = needUpdate;
            this.needRetry = needRetry;
        }
    }

    /**
     * Methods performs operations on zookeeper node responsible for notification of event type changes. In case if it
     * is needed - creates watcher for a node that should trigger refresh.
     *
     * @return A set of flags pointing if the requests should be made again(and it's fine) or cache should be refreshed.
     * @throws Exception In case if there is unexpected zookeeper exception.
     */
    private UpdateRequirement checkUpdateAndListen(final boolean enforceWatcherCreation) throws Exception {
        try {
            final Stat stat = new Stat();
            zooKeeperHolder.get().getData().storingStatIn(stat).forPath(cachePath);

            final boolean updateNeeded = stat.getMzxid() != this.lastZxId;
            if (updateNeeded || enforceWatcherCreation) {
                // Version changed, that means that we have to register listener again, as it was either reaction to a
                // listener or listener failed and we need to recreate it
                zooKeeperHolder.get().getData()
                        .storingStatIn(stat)
                        .usingWatcher((Watcher) this::watcherTriggered)
                        .forPath(cachePath);
                this.lastZxId = stat.getMzxid();
            }

            return new UpdateRequirement(updateNeeded, false);
        } catch (KeeperException.NoNodeException ex) {
            LOG.info("Node {} doesn't exists. Will try to create new one", cachePath);
            try {
                zooKeeperHolder.get().create().creatingParentsIfNeeded().forPath(cachePath, new byte[]{});
                return new UpdateRequirement(false, true);
            } catch (KeeperException.NodeExistsException ex2) {
                LOG.info("Failed to create node {} - it already exists: {}. " +
                        "May happen if several instances are initializing", cachePath, ex2.getMessage());
                return new UpdateRequirement(false, true);
            }
        }

    }

    private void watcherTriggered(final WatchedEvent watchedEvent) {
        updateLock.lock();
        try {
            refreshZookeeper.signalAll();
        } finally {
            updateLock.unlock();
        }
    }

    private void connectionStateChanged(
            final CuratorFramework curatorFramework, final ConnectionState connectionState) {
        updateLock.lock();
        try {
            switch (connectionState) {
                case LOST:
                case RECONNECTED:
                    enforceWatcherCreation = true;
                default:
                    // do nothing (make linter happy)
            }
            refreshZookeeper.signalAll();
        } finally {
            updateLock.unlock();
        }
    }

    private void doPeriodicUpdate() {
        boolean recreateWatcher = false;
        try {
            while (!Thread.currentThread().isInterrupted()) {
                final UpdateRequirement updateRequirement = getUpdatesRequiredCycled(recreateWatcher);

                refreshCacheCycled();
                final boolean zkTriggered;
                updateLock.lock();
                try {
                    zkTriggered = refreshZookeeper.await(forceRefreshMs, TimeUnit.MILLISECONDS);
                    recreateWatcher = this.enforceWatcherCreation;
                    this.enforceWatcherCreation = false;
                } finally {
                    updateLock.unlock();
                }
                if (zkTriggered) {
                    LOG.info("Updating ET cache because zk informed about it");
                } else {
                    LOG.info("Updating ET cache because {} ms interval elapsed", forceRefreshMs);
                }
            }
        } catch (InterruptedException ex) {
            LOG.info("Was interrupted, exiting", ex);
        }
    }

    private void refreshCacheCycled() throws InterruptedException {
        boolean refreshed = false;
        while (!refreshed) {
            try {
                cache.refresh();
                refreshed = true;
            } catch (RuntimeException ex) {
                LOG.warn("Cache fails to refresh. Will wait for {} ms and try again until it succeeds.",
                        CALM_DOWN_PERIOD_MS, ex);
                ThreadUtils.sleep(CALM_DOWN_PERIOD_MS);
            }
        }
    }

    private UpdateRequirement getUpdatesRequiredCycled(final boolean recreateWatcher) throws InterruptedException {
        UpdateRequirement updateRequirement = null;
        while (updateRequirement == null || updateRequirement.needRetry) {
            try {
                updateRequirement = checkUpdateAndListen(recreateWatcher);
            } catch (InterruptedException ex) {
                throw ex;
            } catch (Exception ex) {
                LOG.error("Some kind of weird exception caught while checking zk node. " +
                        "Will sleep a {} ms retry", CALM_DOWN_PERIOD_MS, ex);
                try {
                    ThreadUtils.sleep(CALM_DOWN_PERIOD_MS);
                } catch (InterruptedException ex2) {
                    throw ex2;
                }
            }
        }
        return updateRequirement;
    }

}
