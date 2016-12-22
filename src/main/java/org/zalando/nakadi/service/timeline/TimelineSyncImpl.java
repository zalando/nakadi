package org.zalando.nakadi.service.timeline;

import java.io.Closeable;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Function;
import javax.annotation.Nullable;
import org.apache.curator.framework.recipes.locks.InterProcessSemaphoreMutex;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Profile;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.zalando.nakadi.repository.zookeeper.ZooKeeperHolder;

/**
 * TODO: Reinitialize on zk session recreation.
 */
@Service
@Profile("!test")
public class TimelineSyncImpl implements TimelineSync {
    private static class DelayedChange {
        private final int version;
        private final Set<String> lockedEventTypes;

        private DelayedChange(final int version, final Collection<String> lockedEventTypes) {
            this.version = version;
            this.lockedEventTypes = new HashSet<>(lockedEventTypes);
        }
    }

    private static final String ROOT_PATH = "/timelines";
    private static final Charset CHARSET = Charset.forName("UTF-8");
    private static final Logger LOG = LoggerFactory.getLogger(TimelineSyncImpl.class);

    private final ZooKeeperHolder zooKeeperHolder;
    private final InterProcessSemaphoreMutex lock;
    private final String thisId;
    private final Set<String> lockedEventTypes = new HashSet<>();
    private final Map<String, Integer> eventsBeingPublished = new HashMap<>();
    private final Object lockalLock = new Object();
    private final HashMap<String, List<Consumer<String>>> consumerListeners = new HashMap<>();
    private final List<DelayedChange> queuedChanges = new ArrayList<>();

    @Autowired
    public TimelineSyncImpl(final ZooKeeperHolder zooKeeperHolder) {
        this.zooKeeperHolder = zooKeeperHolder;
        this.thisId = UUID.randomUUID().toString();
        this.lock = new InterProcessSemaphoreMutex(zooKeeperHolder.get(), ROOT_PATH + "/lock");
        this.initializeZkStructure();
    }

    private String toZkPath(final String path) {
        return ROOT_PATH + path;
    }

    private void initializeZkStructure() {
        runLocked(() -> {
            try {
                try {
                    // 1. Create version node, if needed, keep in mind that lock built root path
                    zooKeeperHolder.get().create().withMode(CreateMode.PERSISTENT)
                            .forPath(toZkPath("/version"), "0".getBytes(CHARSET));

                    // 2. Create locked event types structure
                    zooKeeperHolder.get().create().withMode(CreateMode.PERSISTENT)
                            .forPath(toZkPath("/locked_et"), "[]".getBytes(CHARSET));
                    // 3. Create nodes root path
                    zooKeeperHolder.get().create().withMode(CreateMode.PERSISTENT)
                            .forPath(toZkPath("/nodes"), "[]".getBytes(CHARSET));
                } catch (final KeeperException.NodeExistsException ignore) {
                }

                // 4. Get current version and locked event types (and update node state)
                refreshVersionLocked();
                // 5. React on what was received and write node version to zk.
                reactOnEventTypesChange();
            } catch (final Exception e) {
                LOG.error("Failed to initialize subscription api", e);
                throw new RuntimeException(e);
            }
        });
    }

    private <T> T readData(final String relativeName,
                           @Nullable final Watcher watcher, final Function<String, T> converter) throws Exception {
        final byte[] data;
        final String zkPath = toZkPath(relativeName);
        if (null == watcher) {
            data = zooKeeperHolder.get().getData().forPath(zkPath);
        } else {
            data = zooKeeperHolder.get().getData().usingWatcher(watcher).forPath(zkPath);
        }
        return converter.apply(new String(data, CHARSET));
    }

    @Scheduled(fixedDelay = 1000)
    private void reactOnEventTypesChange() throws InterruptedException {
        final Iterator<DelayedChange> it = queuedChanges.iterator();
        while (it.hasNext()) {
            final DelayedChange change = it.next();
            final Set<String> unlockedEventTypes = new HashSet<>();
            synchronized (lockalLock) {
                for (final String item : this.lockedEventTypes) {
                    if (!change.lockedEventTypes.contains(item)) {
                        unlockedEventTypes.add(item);
                    }
                }
                this.lockedEventTypes.clear();
                this.lockedEventTypes.addAll(change.lockedEventTypes);
                boolean haveUsage = true;
                while (haveUsage) {
                    haveUsage = false;
                    for (final String et : this.lockedEventTypes) {
                        if (eventsBeingPublished.containsKey(et)) {
                            haveUsage = true;
                        }
                    }
                    if (haveUsage) {
                        lockalLock.wait();
                    }
                }
                lockalLock.notifyAll();
            }
            // Notify consumers that they should refresh timeline information
            for (final String unlocked : unlockedEventTypes) {
                final List<Consumer<String>> toNotify;
                synchronized (consumerListeners) {
                    toNotify = consumerListeners.containsKey(unlocked) ?
                            new ArrayList<>(consumerListeners.get(unlocked)) : null;
                }
                if (null != toNotify) {
                    for (final Consumer<String> listener : toNotify) {
                        try {
                            listener.accept(unlocked);
                        } catch (final RuntimeException ex) {
                            LOG.error("Failed to notify about event type {} unlock", unlocked, ex);
                        }
                    }
                }
            }
            try {
                zooKeeperHolder.get().setData()
                        .forPath(toZkPath("/nodes/" + thisId), String.valueOf(change.version).getBytes(CHARSET));
            } catch (final RuntimeException ex) {
                throw ex;
            } catch (final Exception ex) {
                throw new RuntimeException(ex);
            }
            it.remove();
        }
    }

    /**
     * MUST be executed under zk lock. Called when version is changed
     */
    private void refreshVersionLocked() {
        try {
            queuedChanges.add(
                    new DelayedChange(readData("/version", this::versionChanged, Integer::parseInt),
                            this.zooKeeperHolder.get().getChildren().forPath(toZkPath("/locked_et"))));
        } catch (final RuntimeException ex) {
            throw ex;
        } catch (final Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    private void versionChanged(final WatchedEvent we) {
        runLocked(this::refreshVersionLocked);
    }

    private void runLocked(final Runnable action) {
        try {
            Exception releaseException = null;
            this.lock.acquire();
            try {
                action.run();
            } finally {
                try {
                    this.lock.release();
                } catch (final Exception ex) {
                    releaseException = ex;
                }
            }
            if (null != releaseException) {
                throw releaseException;
            }
        } catch (final RuntimeException ex) {
            throw ex;
        } catch (final Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    @Override
    public Closeable workWithEventType(final String eventType) throws InterruptedException {
        synchronized (lockalLock) {
            while (lockedEventTypes.contains(eventType)) {
                lockalLock.wait();
            }
            if (!eventsBeingPublished.containsKey(eventType)) {
                eventsBeingPublished.put(eventType, 1);
            } else {
                eventsBeingPublished.put(eventType, 1 + eventsBeingPublished.get(eventType));
            }
        }
        return () -> {
            synchronized (lockalLock) {
                final int currentCount = eventsBeingPublished.get(eventType);
                if (1 == currentCount) {
                    eventsBeingPublished.remove(eventType);
                    lockalLock.notifyAll();
                } else {
                    eventsBeingPublished.put(eventType, currentCount - 1);
                }
            }
        };
    }

    private void updateVersionAndWaitForAllNodes(@Nullable final Long timoutMs) throws InterruptedException {
        // Create next version, that will contain locked event type.
        final AtomicInteger versionToWait = new AtomicInteger();
        runLocked(() -> {
            try {
                final int latestVersion = readData("/version", null, Integer::valueOf);
                versionToWait.set(latestVersion + 1);
                zooKeeperHolder.get().setData()
                        .forPath(toZkPath("/version"), String.valueOf(versionToWait.get()).getBytes(CHARSET));
            } catch (final RuntimeException ex) {
                throw ex;
            } catch (final Exception e) {
                throw new RuntimeException(e);
            }
        });
        // Wait for all nodes to have latest version.
        final AtomicBoolean latestVersionIsThere = new AtomicBoolean(false);
        while (!latestVersionIsThere.get()) {
            Thread.sleep(TimeUnit.SECONDS.toMillis(1));
            runLocked(() -> {
                try {
                    for (final String node : zooKeeperHolder.get().getChildren().forPath(toZkPath("/nodes"))) {
                        final int nodeVersion = readData("/nodes/" + node, null, Integer::valueOf);
                        if (nodeVersion < versionToWait.get()) {
                            LOG.info("Node {} still don't have latest version", node);
                            return;
                        }
                    }
                    latestVersionIsThere.set(true);
                } catch (final RuntimeException e) {
                    throw e;
                } catch (final Exception ex) {
                    throw new RuntimeException(ex);
                }
            });
        }
    }

    @Override
    public void startTimelineUpdate(final String eventType, final long timeoutMs) throws InterruptedException {
        final String etZkPath = toZkPath("/locked_et/" + eventType);
        boolean successfull = false;
        try {
            zooKeeperHolder.get().create().withMode(CreateMode.EPHEMERAL)
                    .forPath(etZkPath, eventType.getBytes(CHARSET));
            updateVersionAndWaitForAllNodes(timeoutMs);
            successfull = true;
        } catch (final InterruptedException ex) {
            throw ex;
        } catch (final Exception e) {
            throw new RuntimeException(e);
        } finally {
            if (!successfull) {
                try {
                    zooKeeperHolder.get().delete().forPath(etZkPath);
                } catch (final Exception e) {
                    LOG.error("Failed to delete node {}", etZkPath, e);
                }
            }
        }
    }

    @Override
    public void finishTimelineUpdate(final String eventType) throws InterruptedException {
        final String etZkPath = toZkPath("/locked_et/" + eventType);
        try {
            zooKeeperHolder.get().delete().forPath(etZkPath);
        } catch (final RuntimeException ex) {
            throw ex;
        } catch (final Exception ex) {
            throw new RuntimeException(ex);
        }
        updateVersionAndWaitForAllNodes(null);
    }

    @Override
    public ListenerRegistration registerTimelineChangeListener(
            final String eventType,
            final Consumer<String> listener) {
        synchronized (consumerListeners) {
            if (!consumerListeners.containsKey(eventType)) {
                consumerListeners.put(eventType, new ArrayList<>());
            }
            consumerListeners.get(eventType).add(listener);
        }
        return () -> {
            synchronized (consumerListeners) {
                consumerListeners.get(eventType).remove(listener);
                if (consumerListeners.get(eventType).isEmpty()) {
                    consumerListeners.remove(eventType);
                }
            }
        };
    }

}
