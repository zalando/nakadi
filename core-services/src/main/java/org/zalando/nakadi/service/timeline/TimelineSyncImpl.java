package org.zalando.nakadi.service.timeline;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Service;
import org.zalando.nakadi.repository.zookeeper.ZooKeeperHolder;

import java.io.Closeable;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

@Service
@Profile("!test")
public class TimelineSyncImpl implements TimelineSync {
    private static final Logger LOG = LoggerFactory.getLogger(TimelineSyncImpl.class);
    private final ZooKeeperHolder zookeeperHolder;
    private final LocalLockManager localLockManager;
    private final ObjectMapper objectMapper;

    @Autowired
    public TimelineSyncImpl(
            final ZooKeeperHolder zooKeeperHolder,
            final LocalLockManager localLockManager,
            final ObjectMapper objectMapper) {
        this.zookeeperHolder = zooKeeperHolder;
        this.localLockManager = localLockManager;
        this.objectMapper = objectMapper;
    }

    @Override
    public Closeable workWithEventType(final String eventType, final long timeoutMs)
            throws InterruptedException, TimeoutException {
        return localLockManager.workWithEventType(eventType, timeoutMs);
    }

    @Override
    public ListenerRegistration registerTimelineChangeListener(
            final String eventType, final Consumer<String> listener) {
        return localLockManager.registerTimelineChangeListener(eventType, listener);
    }

    @Override
    public ListenerRegistration registerTimelineChangeListener(final Consumer<String> listener) {
        return localLockManager.registerTimelineChangeListener(listener);
    }

    /**
     * Applies mutator {@code lockedEtMutator} to set of locked event types using optimistic locking within
     * {@code timeoutMs} timeout.
     *
     * @param lockedEtMutator mutator for event types.
     * @param timeoutMs       timeout to perform update.
     * @return Version, that was generated within this particular mutation.
     */
    private Long applyChangeToState(final Function<Set<String>, Set<String>> lockedEtMutator, final long timeoutMs) {
        final long finishTime = System.currentTimeMillis() + timeoutMs;
        while (timeoutMs == -1 || System.currentTimeMillis() <= finishTime) {
            // First step - read value and remember version it has.
            final Stat stat = new Stat();
            final VersionedLockedEventTypes oldLocked;
            try {
                final byte[] data = zookeeperHolder.get()
                        .getData()
                        .storingStatIn(stat)
                        .forPath(TimelinesConfig.VERSION_PATH);
                oldLocked = VersionedLockedEventTypes.deserialize(objectMapper, data);
            } catch (final Exception ex) {
                if (ex instanceof InterruptedException) {
                    Thread.currentThread().interrupt();
                }
                throw new RuntimeException(ex);
            }

            // Second step - save updated value if version is the same
            final VersionedLockedEventTypes newLocked = new VersionedLockedEventTypes(
                    oldLocked.getVersion() + 1L,
                    lockedEtMutator.apply(oldLocked.getLockedEts())
            );
            try {
                zookeeperHolder.get()
                        .setData()
                        .withVersion(stat.getVersion())
                        .forPath(TimelinesConfig.VERSION_PATH, newLocked.serialize(objectMapper));
                return newLocked.getVersion();
            } catch (final KeeperException.BadVersionException ex) {
                LOG.warn("While running timelines action {} it turned out that node is outdated." +
                        " Will immediately rerun action", lockedEtMutator);
            } catch (final Exception e) {
                throw new RuntimeException(e);
            }

        }
        throw new RuntimeException("Failed to apply " + lockedEtMutator + " within " + timeoutMs + " ms");
    }

    private static class LockedEtMutator implements Function<Set<String>, Set<String>> {
        private final String eventType;
        private final boolean add;

        private LockedEtMutator(final String eventType, final boolean add) {
            this.eventType = eventType;
            this.add = add;
        }

        @Override
        public Set<String> apply(final Set<String> old) {
            final Set<String> newSet = new HashSet<>(old);
            if (add) {
                if (old.contains(eventType)) {
                    throw new RuntimeException("Event type " + eventType + " is already in locked event types");
                }
                newSet.add(eventType);
            } else {
                if (!old.contains(eventType)) {
                    throw new RuntimeException("Event type " + eventType + " is not locked to be unlocked");
                }
                newSet.remove(eventType);
            }
            return newSet;
        }

        @Override
        public String toString() {
            if (add) {
                return "Add event type " + eventType + " to list of locked event types";
            } else {
                return "Remove event type " + eventType + " from list of locked event types";
            }
        }

    }

    @Override
    public void startTimelineUpdate(final String eventType, final long timeoutMs)
            throws InterruptedException, RuntimeException {
        LOG.info("Starting timeline update for event type {} with timeout {} ms", eventType, timeoutMs);
        if (timeoutMs <= 0) {
            throw new IllegalArgumentException("Timeline update can not start withing timeout " + timeoutMs + " ms");
        }
        final long startTime = System.currentTimeMillis();

        final Long newVersion = applyChangeToState(new LockedEtMutator(eventType, true), timeoutMs);

        Exception exceptionCaught = null;
        try {
            waitForAllNodesVersion(newVersion, startTime + timeoutMs - System.currentTimeMillis());
        } catch (InterruptedException | RuntimeException ex) {
            exceptionCaught = ex;
        }
        if (exceptionCaught != null) {
            LOG.warn("Failed to roll wait for nodes to have at least version {}. Rolling back the change", newVersion);
            // We need to rollback the change, and it doesn't matter how long it will take, or et will be locked
            // forever.
            try {
                applyChangeToState(new LockedEtMutator(eventType, false), -1);
            } catch (final RuntimeException ex) {
                LOG.error("Failed to roll back event type {} timeline update. Manual intervention needed.",
                        eventType, ex);
            }
            if (exceptionCaught instanceof InterruptedException) {
                throw (InterruptedException) exceptionCaught;
            } else {
                throw (RuntimeException) exceptionCaught;
            }
        }
    }

    @Override
    public void finishTimelineUpdate(final String eventType) throws InterruptedException, RuntimeException {
        LOG.info("Finishing timeline update for event type {}", eventType);
        final Long version = applyChangeToState(new LockedEtMutator(eventType, false), -1);
        waitForAllNodesVersion(version, TimeUnit.MINUTES.toMillis(1));
    }

    private void waitForAllNodesVersion(final Long version, final long toMillis)
            throws InterruptedException, RuntimeException {
        final long finishAt = System.currentTimeMillis() + toMillis;
        final Map<String, Long> nodesVersions = new HashMap<>();
        while (finishAt > System.currentTimeMillis()) {
            nodesVersions.clear();
            try {
                final List<String> nodes = zookeeperHolder.get().getChildren().forPath(TimelinesConfig.NODES_PATH);
                for (final String node : nodes) {
                    final Long nodeVersion = Long.parseLong(
                            new String(
                                    zookeeperHolder.get().getData().forPath(TimelinesConfig.getNodePath(node)),
                                    StandardCharsets.UTF_8));
                    nodesVersions.put(node, nodeVersion);
                }
            } catch (Exception ex) {
                if (ex instanceof InterruptedException) {
                    throw (InterruptedException) ex;
                }
                throw new RuntimeException(ex);
            }
            final List<Map.Entry<String, Long>> outdated = nodesVersions.entrySet().stream()
                    .filter(e -> e.getValue() < version)
                    .collect(Collectors.toList());
            if (outdated.isEmpty()) {
                // done waiting
                return;
            }
            LOG.warn("Not all the nodes has registered version {}. Outdated nodes: {}",
                    version,
                    outdated.stream().map(n -> n.getKey() + ": " + n.getValue()).collect(Collectors.joining(",")));
            Thread.sleep(100);
        }
        throw new RuntimeException("Failed to wait for all nodes to have at least version " + version +
                ". State: " + nodesVersions.entrySet().stream().map(e -> e.getKey() + "=" + e.getValue())
                .collect(Collectors.joining(", ")));
    }

}
