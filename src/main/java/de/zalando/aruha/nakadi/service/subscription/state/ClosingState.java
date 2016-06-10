package de.zalando.aruha.nakadi.service.subscription.state;

import de.zalando.aruha.nakadi.service.subscription.model.Partition;
import de.zalando.aruha.nakadi.service.subscription.zk.ZKSubscription;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class ClosingState extends State {
    private final Map<Partition.PartitionKey, Long> uncommitedOffsets;
    private final Map<Partition.PartitionKey, ZKSubscription> listeners = new HashMap<>();
    private final long lastCommitMillis;
    private ZKSubscription topologyListener;
    private static final Logger LOG = LoggerFactory.getLogger(ClosingState.class);

    ClosingState(final Map<Partition.PartitionKey, Long> uncommitedOffsets, final long lastCommitMillis) {
        this.uncommitedOffsets = uncommitedOffsets;
        this.lastCommitMillis = lastCommitMillis;
    }

    @Override
    public void onExit() {
        try {
            freePartitions(new HashSet<>(listeners.keySet()));
        } finally {
            if (null != topologyListener) {
                try {
                    topologyListener.cancel();
                } finally {
                    topologyListener = null;
                }
            }
        }
    }

    @Override
    public void onEnter() {
        final long timeToWaitMillis = getParameters().commitTimeoutMillis - (System.currentTimeMillis() - lastCommitMillis);
        if (timeToWaitMillis > 0) {
            scheduleTask(() -> {
                if (isCurrent()) {
                    switchState(new CleanupState());
                }
            }, timeToWaitMillis, TimeUnit.MILLISECONDS);
            topologyListener = getZk().subscribeForTopologyChanges(() -> addTask(this::onTopologyChanged));
            reactOnTopologyChange();
        } else {
            switchState(new CleanupState());
        }
    }

    private void onTopologyChanged() {
        if (!isCurrent()) {
            return;
        }
        topologyListener.refresh();
        reactOnTopologyChange();
    }

    private void reactOnTopologyChange() {
        final Map<Partition.PartitionKey, Partition> partitions = new HashMap<>();
        getZk().runLocked(() -> Stream.of(getZk().listPartitions())
                .filter(p -> getSessionId().equals(p.getSession()))
                .forEach(p -> partitions.put(p.getKey(), p)));
        final Set<Partition.PartitionKey> freeRightNow = new HashSet<>();
        final Set<Partition.PartitionKey> addListeners = new HashSet<>();
        for (final Partition p : partitions.values()) {
            if (Partition.State.REASSIGNING.equals(p.getState())) {
                if (!uncommitedOffsets.containsKey(p.getKey())) {
                    freeRightNow.add(p.getKey());
                } else {
                    if (!listeners.containsKey(p.getKey())) {
                        addListeners.add(p.getKey());
                    }
                }
            } else { // ASSIGNED
                if (uncommitedOffsets.containsKey(p.getKey()) && !listeners.containsKey(p.getKey())) {
                    addListeners.add(p.getKey());
                }
            }
        }
        uncommitedOffsets.keySet().stream().filter(p -> !partitions.containsKey(p)).forEach(freeRightNow::add);
        freePartitions(freeRightNow);
        addListeners.forEach(this::registerListener);
        tryCompleteState();
    }

    private void registerListener(final Partition.PartitionKey key) {
        listeners.put(
                key,
                getZk().subscribeForOffsetChanges(
                        key, () -> addTask(() -> this.offsetChanged(key))));
        reactOnOffset(key);
    }

    private void offsetChanged(final Partition.PartitionKey key) {
        if (listeners.containsKey(key)) {
            listeners.get(key).refresh();
        }
        reactOnOffset(key);
    }

    private void reactOnOffset(final Partition.PartitionKey key) {
        if (!isCurrent()) {
            return;
        }
        final long newOffset = getZk().getOffset(key);
        if (uncommitedOffsets.containsKey(key) && uncommitedOffsets.get(key) <= newOffset) {
            freePartitions(Collections.singletonList(key));
        }
        tryCompleteState();
    }

    private void tryCompleteState() {
        if (isCurrent() && uncommitedOffsets.isEmpty()) {
            switchState(new CleanupState());
        }
    }

    private void freePartitions(final Collection<Partition.PartitionKey> keys) {
        RuntimeException exceptionCaught = null;
        for (final Partition.PartitionKey partitionKey : keys) {
            uncommitedOffsets.remove(partitionKey);
            final ZKSubscription listener = listeners.remove(partitionKey);
            if (null != listener) {
                try {
                    listener.cancel();
                } catch (final RuntimeException ex) {
                    exceptionCaught = ex;
                    LOG.error("Failed to cancel offsets listener" + listener, ex);
                }
            }
        }
        getZk().runLocked(() -> getZk().transfer(getSessionId(), keys));
        if (null != exceptionCaught) {
            throw exceptionCaught;
        }
    }
}
