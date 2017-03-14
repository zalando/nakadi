package org.zalando.nakadi.service.subscription.state;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.LongSupplier;
import java.util.function.Supplier;
import java.util.stream.Stream;
import org.zalando.nakadi.domain.NakadiCursor;
import org.zalando.nakadi.domain.TopicPartition;
import org.zalando.nakadi.service.subscription.model.Partition;
import org.zalando.nakadi.service.subscription.zk.ZKSubscription;

class ClosingState extends State {
    private final Supplier<Map<TopicPartition, NakadiCursor>> uncommittedOffsetsSupplier;
    private final LongSupplier lastCommitSupplier;
    private Map<TopicPartition, NakadiCursor> uncommittedOffsets;
    private final Map<TopicPartition, ZKSubscription> listeners = new HashMap<>();
    private ZKSubscription topologyListener;

    ClosingState(final Supplier<Map<TopicPartition, NakadiCursor>> uncommittedOffsetsSupplier,
                 final LongSupplier lastCommitSupplier) {
        this.uncommittedOffsetsSupplier = uncommittedOffsetsSupplier;
        this.lastCommitSupplier = lastCommitSupplier;
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
        final long timeToWaitMillis = getParameters().commitTimeoutMillis -
                (System.currentTimeMillis() - lastCommitSupplier.getAsLong());
        uncommittedOffsets = uncommittedOffsetsSupplier.get();
        if (!uncommittedOffsets.isEmpty() && timeToWaitMillis > 0) {
            scheduleTask(() -> switchState(new CleanupState()), timeToWaitMillis, TimeUnit.MILLISECONDS);
            topologyListener = getZk().subscribeForTopologyChanges(() -> addTask(this::onTopologyChanged));
            reactOnTopologyChange();
        } else {
            switchState(new CleanupState());
        }
    }

    private void onTopologyChanged() {
        if (topologyListener == null) {
            throw new IllegalStateException(
                    "topologyListener should not be null when calling onTopologyChanged method");
        }
        topologyListener.refresh();
        reactOnTopologyChange();
    }

    private void reactOnTopologyChange() {

        // Collect current partitions state from Zk
        final Map<TopicPartition, Partition> partitions = new HashMap<>();
        getZk().runLocked(() -> Stream.of(getZk().listPartitions())
                .filter(p -> getSessionId().equals(p.getSession()))
                .forEach(p -> partitions.put(p.getKey(), p)));
        // Select which partitions need to be freed from this session
        // Ithere
        final Set<TopicPartition> freeRightNow = new HashSet<>();
        final Set<TopicPartition> addListeners = new HashSet<>();
        for (final Partition p : partitions.values()) {
            if (Partition.State.REASSIGNING.equals(p.getState())) {
                if (!uncommittedOffsets.containsKey(p.getKey())) {
                    freeRightNow.add(p.getKey());
                } else {
                    if (!listeners.containsKey(p.getKey())) {
                        addListeners.add(p.getKey());
                    }
                }
            } else { // ASSIGNED
                if (uncommittedOffsets.containsKey(p.getKey()) && !listeners.containsKey(p.getKey())) {
                    addListeners.add(p.getKey());
                }
            }
        }
        uncommittedOffsets.keySet().stream().filter(p -> !partitions.containsKey(p)).forEach(freeRightNow::add);
        freePartitions(freeRightNow);
        addListeners.forEach(this::registerListener);
        tryCompleteState();
    }

    private void registerListener(final TopicPartition key) {
        listeners.put(
                key,
                getZk().subscribeForOffsetChanges(
                        key, () -> addTask(() -> this.offsetChanged(key))));
        reactOnOffset(key);
    }

    private void offsetChanged(final TopicPartition key) {
        if (listeners.containsKey(key)) {
            listeners.get(key).refresh();
        }
        reactOnOffset(key);
    }

    private void reactOnOffset(final TopicPartition key) {
        final String newOffset = getZk().getOffset(key);
        if (uncommittedOffsets.containsKey(key) && uncommittedOffsets.get(key).getOffset().compareTo(newOffset) <= 0) {
            freePartitions(Collections.singletonList(key));
        }
        tryCompleteState();
    }

    private void tryCompleteState() {
        if (uncommittedOffsets.isEmpty()) {
            switchState(new CleanupState());
        }
    }

    private void freePartitions(final Collection<TopicPartition> keys) {
        RuntimeException exceptionCaught = null;
        for (final TopicPartition partitionKey : keys) {
            uncommittedOffsets.remove(partitionKey);
            final ZKSubscription listener = listeners.remove(partitionKey);
            if (null != listener) {
                try {
                    listener.cancel();
                } catch (final RuntimeException ex) {
                    exceptionCaught = ex;
                    getLog().error("Failed to cancel offsets listener {}", listener, ex);
                }
            }
        }
        getZk().runLocked(() -> getZk().transfer(getSessionId(), keys));
        if (null != exceptionCaught) {
            throw exceptionCaught;
        }
    }
}
