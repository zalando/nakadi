package org.zalando.nakadi.service.timeline;

import java.io.Closeable;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.apache.curator.framework.recipes.locks.InterProcessLock;
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
import org.zalando.nakadi.util.UUIDGenerator;

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

        @Override
        public String toString() {
            return "{version=" + version + ", lockedEventTypes=" + lockedEventTypes + '}';
        }
    }

    private static final String ROOT_PATH = "/timelines";
    private static final Charset CHARSET = Charset.forName("UTF-8");

    private final ZooKeeperHolder zooKeeperHolder;
    private final String thisId;
    private final Set<String> lockedEventTypes = new HashSet<>();
    private final Map<String, Integer> eventsBeingPublished = new HashMap<>();
    private final Object localLock = new Object();
    private final HashMap<String, List<Consumer<String>>> consumerListeners = new HashMap<>();
    private final BlockingQueue<DelayedChange> queuedChanges = new LinkedBlockingQueue<>();
    private final Logger log;

    @Autowired
    public TimelineSyncImpl(final ZooKeeperHolder zooKeeperHolder, final UUIDGenerator uuidGenerator) {
        this.thisId = uuidGenerator.randomUUID().toString();
        this.log = LoggerFactory.getLogger("timelines.sync." + thisId);
        this.zooKeeperHolder = zooKeeperHolder;
        this.initializeZkStructure();
    }

    @Override
    public String getNodeId() {
        return thisId;
    }

    private String toZkPath(final String path) {
        return ROOT_PATH + path;
    }

    private void initializeZkStructure() {
        log.info("Starting initialization");
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

                log.info("Registering node in zk");
                zooKeeperHolder.get().create().withMode(CreateMode.EPHEMERAL)
                        .forPath(toZkPath("/nodes/" + thisId), "0".getBytes(CHARSET));

                // 4. Get current version and locked event types (and update node state)
                refreshVersion();
                // 5. React on what was received and write node version to zk.
                reactOnEventTypesChange();
            } catch (final Exception e) {
                log.error("Failed to initialize timeline synchronization", e);
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

    @Scheduled(fixedDelay = 500)
    public void reactOnEventTypesChange() throws InterruptedException {
        while (null != queuedChanges.peek()) {
            final DelayedChange change = queuedChanges.peek();
            log.info("Reacting on delayed change {}", change);
            final Set<String> unlockedEventTypes = new HashSet<>();
            synchronized (localLock) {
                for (final String item : this.lockedEventTypes) {
                    if (!change.lockedEventTypes.contains(item)) {
                        unlockedEventTypes.add(item);
                    }
                }
                this.lockedEventTypes.clear();
                this.lockedEventTypes.addAll(change.lockedEventTypes);
                boolean haveUsage = true;
                while (haveUsage) {
                    final List<String> stillLocked = this.lockedEventTypes.stream()
                            .filter(eventsBeingPublished::containsKey).collect(Collectors.toList());
                    haveUsage = !stillLocked.isEmpty();
                    if (haveUsage) {
                        log.info("Event types are still locked: {}", stillLocked);
                        localLock.wait();
                    }
                }
                localLock.notifyAll();
            }
            // Notify consumers that they should refresh timeline information
            for (final String unlocked : unlockedEventTypes) {
                log.info("Notifying about unlock of {}", unlocked);
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
                            log.error("Failed to notify about event type {} unlock", unlocked, ex);
                        }
                    }
                }
            }
            try {
                zooKeeperHolder.get().setData()
                        .forPath(toZkPath("/nodes/" + thisId), String.valueOf(change.version).getBytes(CHARSET));
            } catch (final Exception ex) {
                log.error("Failed to update node version in zk. Will try to reprocess again", ex);
                return;
            }
            queuedChanges.poll();
            log.info("Delayed change {} successfully processed", change);
        }
    }

    /**
     * MUST be executed under zk lock. Called when version is changed
     */
    private void refreshVersion() {
        try {
            log.info("Adding refresh call to delayed changes list");
            queuedChanges.add(
                    new DelayedChange(readData("/version", this::versionChanged, Integer::parseInt),
                            this.zooKeeperHolder.get().getChildren().forPath(toZkPath("/locked_et"))));
        } catch (final RuntimeException ex) {
            throw ex;
        } catch (final Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    private void versionChanged(final WatchedEvent ignore) {
        refreshVersion();
    }

    private void runLocked(final Runnable action) {
        try {
            Exception releaseException = null;
            final InterProcessLock lock =
                    new InterProcessSemaphoreMutex(zooKeeperHolder.get(), ROOT_PATH + "/lock");
            lock.acquire();
            try {
                action.run();
            } finally {
                try {
                    lock.release();
                } catch (final Exception ex) {
                    log.warn("Failed to release lock", ex);
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
        synchronized (localLock) {
            while (lockedEventTypes.contains(eventType)) {
                localLock.wait();
            }
            if (!eventsBeingPublished.containsKey(eventType)) {
                eventsBeingPublished.put(eventType, 1);
            } else {
                eventsBeingPublished.put(eventType, 1 + eventsBeingPublished.get(eventType));
            }
        }
        return () -> {
            synchronized (localLock) {
                final int currentCount = eventsBeingPublished.get(eventType);
                if (1 == currentCount) {
                    eventsBeingPublished.remove(eventType);
                    localLock.notifyAll();
                } else {
                    eventsBeingPublished.put(eventType, currentCount - 1);
                }
            }
        };
    }

    private void updateVersionAndWaitForAllNodes(@Nullable final Long timeoutMs) throws InterruptedException {
        // Create next version, that will contain locked event type.
        final Optional<Long> expectedFinish = Optional.ofNullable(timeoutMs).map(t -> System.currentTimeMillis() + t);
        final AtomicInteger versionToWait = new AtomicInteger();
        runLocked(() -> {
            try {
                final int latestVersion = readData("/version", null, Integer::valueOf);
                versionToWait.set(latestVersion + 1);
                zooKeeperHolder.get().setData()
                        .forPath(toZkPath("/version"), String.valueOf(versionToWait.get()).getBytes(CHARSET));
                log.info("Wrote to ZK version to wait (was: {}), new one: {}", latestVersion, versionToWait.get());
            } catch (final RuntimeException ex) {
                throw ex;
            } catch (final Exception e) {
                throw new RuntimeException(e);
            }
        });
        // Wait for all nodes to have latest version.
        final AtomicBoolean latestVersionIsThere = new AtomicBoolean(false);
        while (!latestVersionIsThere.get()) {
            log.info("Waiting for all nodes to have the same version {}", versionToWait.get());
            if (expectedFinish.isPresent() && System.currentTimeMillis() > expectedFinish.get()) {
                throw new RuntimeException("Timed out while updating version to " + versionToWait.get());
            }
            Thread.sleep(TimeUnit.SECONDS.toMillis(1));
            runLocked(() -> {
                try {
                    final List<String> nodes = zooKeeperHolder.get().getChildren().forPath(toZkPath("/nodes"));
                    boolean allOk = true;
                    for (final String node : nodes) {
                        final int nodeVersion = readData("/nodes/" + node, null, Integer::valueOf);
                        final boolean nodeOk = nodeVersion >= versionToWait.get();
                        if (!nodeOk) {
                            allOk = false;
                        }
                        log.info("Node {} OK (current: {}, expected: {})", node, nodeVersion, versionToWait.get());
                    }
                    if (allOk) {
                        latestVersionIsThere.set(true);
                    }
                } catch (final RuntimeException e) {
                    throw e;
                } catch (final Exception ex) {
                    throw new RuntimeException(ex);
                }
            });
        }
        log.info("Version update to {} complete", versionToWait.get());
    }

    @Override
    public void startTimelineUpdate(final String eventType, final long timeoutMs) throws InterruptedException {
        log.info("Starting timeline update for event type {} with timeout {} ms", eventType, timeoutMs);
        final String etZkPath = toZkPath("/locked_et/" + eventType);
        boolean successfull = false;
        try {
            zooKeeperHolder.get().create().withMode(CreateMode.EPHEMERAL)
                    .forPath(etZkPath, thisId.getBytes(CHARSET));
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
                    log.error("Failed to delete node {}", etZkPath, e);
                }
            }
        }
    }

    @Override
    public void finishTimelineUpdate(final String eventType) throws InterruptedException {
        log.info("Finishing timeline update for event type {}", eventType);
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
