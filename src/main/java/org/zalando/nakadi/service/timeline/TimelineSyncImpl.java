package org.zalando.nakadi.service.timeline;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Charsets;
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
import org.zalando.nakadi.util.ThreadUtils;
import org.zalando.nakadi.util.UUIDGenerator;

import javax.annotation.Nullable;
import java.io.Closeable;
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
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Function;

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

    private static final String ROOT_PATH = "/nakadi/timelines";
    private static final Logger LOG = LoggerFactory.getLogger(TimelineSyncImpl.class);
    private static final long LOCK_ZK_TIMEOUT = 10_000;

    private final ZooKeeperHolder zooKeeperHolder;
    private final String nodeId;
    private final LocalLocking localLocking = new LocalLocking();
    private final Map<String, List<Consumer<String>>> consumerListeners = new HashMap<>();
    private final BlockingQueue<DelayedChange> queuedChanges = new LinkedBlockingQueue<>();
    private final AtomicBoolean newVersionPresent = new AtomicBoolean(true);

    @Autowired
    public TimelineSyncImpl(final ZooKeeperHolder zooKeeperHolder, final UUIDGenerator uuidGenerator)
            throws InterruptedException {
        this.nodeId = uuidGenerator.randomUUID().toString();
        this.zooKeeperHolder = zooKeeperHolder;
        this.initializeZkStructure();
    }

    @VisibleForTesting
    public String getNodeId() {
        return nodeId;
    }

    private String toZkPath(final String path) {
        return ROOT_PATH + path;
    }

    private void initializeZkStructure() throws InterruptedException {
        LOG.info("Starting initialization");
        if (!checkZkStructureInited()) {
            try {
                // 1. Create version node, if needed, keep in mind that lock built root path
                zooKeeperHolder.get().create().creatingParentsIfNeeded().withMode(CreateMode.PERSISTENT)
                        .forPath(toZkPath("/version"), "0".getBytes(Charsets.UTF_8));

                // 2. Create locked event types structure
                zooKeeperHolder.get().create().creatingParentsIfNeeded().withMode(CreateMode.PERSISTENT)
                        .forPath(toZkPath("/locked_et"), "[]".getBytes(Charsets.UTF_8));
                // 3. Create nodes root path
                zooKeeperHolder.get().create().creatingParentsIfNeeded().withMode(CreateMode.PERSISTENT)
                        .forPath(toZkPath("/nodes"), "[]".getBytes(Charsets.UTF_8));
            } catch (final KeeperException.NodeExistsException ignore) {
                // skip it, node was already created
            } catch (final Exception e) {
                LOG.error("Failed to check zk structures", e);
                throw new RuntimeException(e);
            }
        }

        runLocked(() -> {
            try {
                LOG.info("Registering node in zk");
                updateSelfVersionTo(0);
            } catch (final Exception e) {
                LOG.error("Failed to initialize timeline synchronization", e);
                throw new RuntimeException(e);
            }
        });
        // 4. React on what was received and write node version to zk.
        reactOnEventTypesChange();
    }

    private boolean checkZkStructureInited() {
        try {
            return zooKeeperHolder.get().checkExists().forPath(toZkPath("/version")) != null &&
                    zooKeeperHolder.get().checkExists().forPath(toZkPath("/locked_et")) != null &&
                    zooKeeperHolder.get().checkExists().forPath(toZkPath("/nodes")) != null;
        } catch (final Exception e) {
            // was not able to check or node is not there
        }
        return false;
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
        return converter.apply(new String(data, Charsets.UTF_8));
    }

    @Scheduled(fixedDelay = 500)
    public void reactOnEventTypesChange() throws InterruptedException {
        checkForNewChange();
        while (!queuedChanges.isEmpty()) {
            final DelayedChange change = queuedChanges.peek();
            LOG.info("Reacting on delayed change {}", change);
            final Set<String> unlockedEventTypes = localLocking.getUnlockedEventTypes(change.lockedEventTypes);
            // Notify consumers that they should refresh timeline information
            for (final String unlocked : unlockedEventTypes) {
                LOG.info("Notifying about unlock of {}", unlocked);
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
            // Updating the list of locked event types is done only after updating the cache in order to guarantee that
            // there is no concurrency between publisher threads and cache expire thread, which has lead to events being
            // published to the wrong timeline. More details in ARUHA-1359.
            localLocking.updateLockedEventTypes(change.lockedEventTypes);
            try {
                updateSelfVersionTo(change.version);
            } catch (final Exception ex) {
                LOG.error("Failed to update node version in zk. Will try to reprocess again", ex);
                return;
            }
            queuedChanges.poll();
            LOG.info("Delayed change {} successfully processed", change);
        }
    }

    private void updateSelfVersionTo(final int version) throws Exception {
        final String zkPath = toZkPath("/nodes/" + nodeId);
        final byte[] versionBytes = String.valueOf(version).getBytes(Charsets.UTF_8);
        try {
            zooKeeperHolder.get().setData().forPath(zkPath, versionBytes);
        } catch (final KeeperException.NoNodeException ex) {
            zooKeeperHolder.get().create().withMode(CreateMode.EPHEMERAL).forPath(zkPath, versionBytes);
        }
    }

    private void checkForNewChange() {
        if (newVersionPresent.compareAndSet(true, false)) {
            runLocked(() -> {
                boolean success = false;
                try {
                    queuedChanges.add(
                            new DelayedChange(readData("/version", this::versionChanged, Integer::parseInt),
                                    this.zooKeeperHolder.get().getChildren().forPath(toZkPath("/locked_et"))));
                    success = true;
                } catch (final RuntimeException ex) {
                    throw ex;
                } catch (final InterruptedException ex) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException(ex);
                } catch (final Exception ex) {
                    throw new RuntimeException(ex);
                } finally {
                    if (!success) {
                        newVersionPresent.set(true);
                    }
                }
            });
        }
    }

    private void versionChanged(final WatchedEvent ignore) {
        LOG.info("Adding refresh call to delayed changes list");
        newVersionPresent.set(true);
    }

    private void runLocked(final Runnable action) {
        try {
            Exception releaseException = null;
            final InterProcessLock lock =
                    new InterProcessSemaphoreMutex(zooKeeperHolder.get(), ROOT_PATH + "/lock");
            final boolean acquired = lock.acquire(LOCK_ZK_TIMEOUT, TimeUnit.MILLISECONDS);
            if (!acquired) {
                throw new RuntimeException(String.format("Lock can not be acquired in %d ms", LOCK_ZK_TIMEOUT));
            }
            try {
                action.run();
            } finally {
                try {
                    lock.release();
                } catch (final Exception ex) {
                    LOG.error("Failed to release lock", ex);
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
    public Closeable workWithEventType(final String eventType, final long timeoutMs)
            throws InterruptedException, TimeoutException {
        return localLocking.workWithEventType(eventType, timeoutMs);
    }

    private void updateVersionAndWaitForAllNodes(@Nullable final Long timeoutMs)
            throws InterruptedException, RuntimeException {
        // Create next version, that will contain locked event type.
        final Optional<Long> expectedFinish = Optional.ofNullable(timeoutMs).map(t -> System.currentTimeMillis() + t);
        final AtomicInteger versionToWait = new AtomicInteger();
        runLocked(() -> {
            try {
                final int latestVersion = readData("/version", null, Integer::valueOf);
                versionToWait.set(latestVersion + 1);
                zooKeeperHolder.get().setData().forPath(
                        toZkPath("/version"), String.valueOf(versionToWait.get()).getBytes(Charsets.UTF_8));
                LOG.info("Wrote to ZK version to wait (was: {}), new one: {}", latestVersion, versionToWait.get());
            } catch (final RuntimeException ex) {
                throw ex;
            } catch (final Exception e) {
                throw new RuntimeException(e);
            }
        });
        // Wait for all nodes to have latest version.
        final AtomicBoolean latestVersionIsThere = new AtomicBoolean(false);
        while (!latestVersionIsThere.get()) {
            LOG.info("Waiting for all nodes to have the same version {}", versionToWait.get());
            if (expectedFinish.isPresent() && System.currentTimeMillis() > expectedFinish.get()) {
                throw new RuntimeException("Timed out while updating version to " + versionToWait.get());
            }
            ThreadUtils.sleep(TimeUnit.SECONDS.toMillis(1));
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
                        LOG.info("Node {} OK (current: {}, expected: {})", node, nodeVersion, versionToWait.get());
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
        LOG.info("Version update to {} complete", versionToWait.get());
    }

    @Override
    public void startTimelineUpdate(final String eventType, final long timeoutMs)
            throws InterruptedException, RuntimeException {
        LOG.info("Starting timeline update for event type {} with timeout {} ms", eventType, timeoutMs);
        final String etZkPath = toZkPath("/locked_et/" + eventType);

        try {
            zooKeeperHolder.get().create().withMode(CreateMode.EPHEMERAL)
                    .forPath(etZkPath, nodeId.getBytes(Charsets.UTF_8));
        } catch (final KeeperException.NodeExistsException ex) {
            throw new IllegalStateException(ex);
        } catch (final Exception ex) {
            throw new RuntimeException(ex);
        }

        boolean successful = false;
        try {
            updateVersionAndWaitForAllNodes(timeoutMs);
            successful = true;
        } catch (final InterruptedException ex) {
            throw ex;
        } finally {
            if (!successful) {
                try {
                    zooKeeperHolder.get().delete().forPath(etZkPath);
                } catch (final Exception e) {
                    LOG.error("Failed to delete node {}", etZkPath, e);
                }
            }
        }
    }

    @Override
    public void finishTimelineUpdate(final String eventType) throws InterruptedException, RuntimeException {
        LOG.info("Finishing timeline update for event type {}", eventType);
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
