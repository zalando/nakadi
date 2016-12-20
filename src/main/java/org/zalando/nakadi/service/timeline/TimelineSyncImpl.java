package org.zalando.nakadi.service.timeline;

import org.apache.curator.framework.recipes.locks.InterProcessSemaphoreMutex;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.zalando.nakadi.repository.zookeeper.ZooKeeperHolder;

import javax.annotation.Nullable;
import java.io.Closeable;
import java.nio.charset.Charset;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * TODO: Reinitialize on zk session recreation.
 */
@Service
public class TimelineSyncImpl implements TimelineSync {
    private static class DelayedChange {
        private final int version;
        private final Set<String> lockedEventTypes;

        private DelayedChange(int version, Collection<String> lockedEventTypes) {
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
    public TimelineSyncImpl(ZooKeeperHolder zooKeeperHolder) {
        this.zooKeeperHolder = zooKeeperHolder;
        this.thisId = UUID.randomUUID().toString();
        this.lock = new InterProcessSemaphoreMutex(zooKeeperHolder.get(), ROOT_PATH + "/lock");
        this.initializeZkStructure();
    }

    private String toZkPath(String path) {
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
                } catch (KeeperException.NodeExistsException ignore) {
                }

                // 4. Get current version and locked event types (and update node state)
                refreshVersionLocked();
                // 5. React on what was received and write node version to zk.
                reactOnEventTypesChange();
            } catch (Exception e) {
                LOG.error("Failed to initialize subscription api", e);
                throw new RuntimeException(e);
            }
        });
    }

    private <T> T readData(final String relativeName, @Nullable final Watcher watcher, final Function<String, T> converter) throws Exception {
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
                for (String item : this.lockedEventTypes) {
                    if (!change.lockedEventTypes.contains(item)) {
                        unlockedEventTypes.add(item);
                    }
                }
                this.lockedEventTypes.clear();
                this.lockedEventTypes.addAll(change.lockedEventTypes);
                boolean haveUsage = true;
                while (haveUsage) {
                    haveUsage = false;
                    for (String et : this.lockedEventTypes) {
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
            for (String unlocked : unlockedEventTypes) {
                final List<Consumer<String>> toNotify;
                synchronized (consumerListeners) {
                    toNotify = consumerListeners.containsKey(unlocked) ? new ArrayList<>(consumerListeners.get(unlocked)) : null;
                }
                if (null != toNotify) {
                    for (Consumer<String> listener : toNotify) {
                        try {
                            listener.accept(unlocked);
                        } catch (RuntimeException ex) {
                            LOG.error("Failed to notify about event type {} unlock", unlocked, ex);
                        }
                    }
                }
            }
            try {
                zooKeeperHolder.get().setData()
                        .forPath(toZkPath("/nodes/" + thisId), String.valueOf(change.version).getBytes(CHARSET));
            } catch (RuntimeException ex) {
                throw ex;
            } catch (Exception ex) {
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
        } catch (RuntimeException ex) {
            throw ex;
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    private void versionChanged(WatchedEvent we) {
        runLocked(this::refreshVersionLocked);
    }

    private void runLocked(Runnable action) {
        try {
            Exception releaseException = null;
            this.lock.acquire();
            try {
                action.run();
            } finally {
                try {
                    this.lock.release();
                } catch (Exception ex) {
                    releaseException = ex;
                }
            }
            if (null != releaseException) {
                throw releaseException;
            }
        } catch (RuntimeException ex) {
            throw ex;
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    @Override
    public Closeable workWithEventType(String eventType) throws InterruptedException {
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

    private void updateVersionAndWaitForAllNodes(@Nullable Long timoutMs) throws InterruptedException {
        // Create next version, that will contain locked event type.
        final AtomicInteger versionToWait = new AtomicInteger();
        runLocked(() -> {
            try {
                final int latestVersion = readData("/version", null, Integer::valueOf);
                versionToWait.set(latestVersion + 1);
                zooKeeperHolder.get().setData()
                        .forPath(toZkPath("/version"), String.valueOf(versionToWait.get()).getBytes(CHARSET));
            } catch (RuntimeException ex) {
                throw ex;
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
        // Wait for all nodes to have latest version.
        AtomicBoolean latestVersionIsThere = new AtomicBoolean(false);
        while (!latestVersionIsThere.get()) {
            Thread.sleep(TimeUnit.SECONDS.toMillis(1));
            runLocked(() -> {
                try {
                    for (String node : zooKeeperHolder.get().getChildren().forPath(toZkPath("/nodes"))) {
                        final int nodeVersion = readData("/nodes/" + node, null, Integer::valueOf);
                        if (nodeVersion < versionToWait.get()) {
                            LOG.info("Node {} still don't have latest version", node);
                            return;
                        }
                    }
                    latestVersionIsThere.set(true);
                } catch (RuntimeException e) {
                    throw e;
                } catch (Exception ex) {
                    throw new RuntimeException(ex);
                }
            });
        }
    }

    @Override
    public void startTimelineUpdate(String eventType, long timeoutMs) throws InterruptedException {
        final String etZkPath = toZkPath("/locked_et/" + eventType);
        boolean successfull = false;
        try {
            zooKeeperHolder.get().create().withMode(CreateMode.EPHEMERAL)
                    .forPath(etZkPath, eventType.getBytes(CHARSET));
            updateVersionAndWaitForAllNodes(timeoutMs);
            successfull = true;
        } catch (InterruptedException ex) {
            throw ex;
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            if (!successfull) {
                try {
                    zooKeeperHolder.get().delete().forPath(etZkPath);
                } catch (Exception e) {
                    LOG.error("Failed to delete node {}", etZkPath, e);
                }
            }
        }
    }

    @Override
    public void finishTimelineUpdate(String eventType) throws InterruptedException {
        final String etZkPath = toZkPath("/locked_et/" + eventType);
        try {
            zooKeeperHolder.get().delete().forPath(etZkPath);
        } catch (RuntimeException ex) {
            throw ex;
        } catch (Exception ex) {
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
