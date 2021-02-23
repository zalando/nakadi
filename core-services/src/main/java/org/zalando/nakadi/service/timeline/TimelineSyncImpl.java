package org.zalando.nakadi.service.timeline;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Service;

import java.io.Closeable;
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
    private final TimelinesZookeeper timelinesZookeeper;
    private final LocalLockManager localLockManager;

    @Autowired
    public TimelineSyncImpl(
            final TimelinesZookeeper timelinesZookeeper,
            final LocalLockManager localLockManager) {
        this.timelinesZookeeper = timelinesZookeeper;
        this.localLockManager = localLockManager;
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
    private Long applyChangeToState(final Function<Set<String>, Set<String>> lockedEtMutator, final long timeoutMs)
            throws InterruptedException {
        final long finishTime = System.currentTimeMillis() + timeoutMs;
        while (timeoutMs == -1 || System.currentTimeMillis() <= finishTime) {
            // First step - read value and remember version it has.
            final TimelinesZookeeper.ZkVersionedLockedEventTypes oldVersion =
                    timelinesZookeeper.getCurrentVersion(null);

            // Second step - save updated value if version is the same
            final VersionedLockedEventTypes newLocked = new VersionedLockedEventTypes(
                    oldVersion.data.getVersion() + 1L,
                    lockedEtMutator.apply(oldVersion.data.getLockedEts())
            );
            final boolean updateSucceeded = timelinesZookeeper.setCurrentVersion(newLocked, oldVersion.zkVersion);
            LOG.info("Set current version to {} and update succeeded: {}", newLocked.getVersion(), updateSucceeded);
            if (updateSucceeded) {
                return newLocked.getVersion();
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
        // In case if this method is not called (or fails) - the only way to roll back - go manually to zk and update
        // version in the verion node (and probably remove event type from locked event types).
        LOG.info("Finishing timeline update for event type {}", eventType);
        final Long version = applyChangeToState(new LockedEtMutator(eventType, false), -1);
        waitForAllNodesVersion(version, TimeUnit.MINUTES.toMillis(1));
        LOG.info("Timeline update for event type {} finished", eventType);
    }

    private void waitForAllNodesVersion(final Long version, final long toMillis)
            throws InterruptedException, RuntimeException {
        final long finishAt = System.currentTimeMillis() + toMillis;
        Map<String, Long> nodesVersions = new HashMap<>();
        LOG.info("Waiting for all nodes to have the same version within {} ms", toMillis);
        while (finishAt > System.currentTimeMillis()) {
            nodesVersions = timelinesZookeeper.getNodesVersions();
            final List<Map.Entry<String, Long>> outdated = nodesVersions.entrySet().stream()
                    .filter(e -> e.getValue() < version)
                    .collect(Collectors.toList());
            if (outdated.isEmpty()) {
                LOG.info("Finished waiting for all {} nodes to have version {}", nodesVersions.size(), version);
                return;
            }
            LOG.warn("Not all the nodes have registered version {}. Outdated nodes: {}",
                    version,
                    outdated.stream().map(n -> n.getKey() + ": " + n.getValue()).collect(Collectors.joining(",")));
            Thread.sleep(100);
        }
        throw new RuntimeException("Failed to wait for all nodes to have at least version " + version +
                ". State: " + nodesVersions.entrySet().stream().map(e -> e.getKey() + "=" + e.getValue())
                .collect(Collectors.joining(", ")));
    }

}
