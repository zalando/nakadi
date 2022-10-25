package org.zalando.nakadi.service.subscription.state;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zalando.nakadi.domain.EventTypePartition;
import org.zalando.nakadi.domain.NakadiCursor;
import org.zalando.nakadi.exceptions.runtime.NakadiBaseException;
import org.zalando.nakadi.exceptions.runtime.NakadiRuntimeException;
import org.zalando.nakadi.service.subscription.model.Partition;
import org.zalando.nakadi.service.subscription.zk.ZkSubscription;
import org.zalando.nakadi.service.subscription.zk.ZkSubscriptionClient;
import org.zalando.nakadi.view.SubscriptionCursorWithoutToken;

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

class ClosingState extends State {
    private static final Logger LOG = LoggerFactory.getLogger(ClosingState.class);

    private final Supplier<Map<EventTypePartition, NakadiCursor>> uncommittedOffsetsSupplier;
    private final LongSupplier lastCommitSupplier;
    private Map<EventTypePartition, NakadiCursor> uncommittedOffsets;
    private final Map<EventTypePartition, ZkSubscription<SubscriptionCursorWithoutToken>> listeners = new HashMap<>();
    private ZkSubscription<ZkSubscriptionClient.Topology> topologyListener;

    ClosingState(final Supplier<Map<EventTypePartition, NakadiCursor>> uncommittedOffsetsSupplier,
                 final LongSupplier lastCommitSupplier) {
        this.uncommittedOffsetsSupplier = uncommittedOffsetsSupplier;
        this.lastCommitSupplier = lastCommitSupplier;
    }

    @Override
    public void onExit() {
        try {
            getAutocommit().autocommit();
            freePartitions(new HashSet<>(listeners.keySet()));
        } catch (final NakadiRuntimeException | NakadiBaseException ex) {
            // In order not to stuck here one will just log this exception, without rethrowing
            LOG.error("Failed to transfer partitions when leaving ClosingState", ex);
        } finally {
            if (null != topologyListener) {
                try {
                    topologyListener.close();
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
            try {
                topologyListener = getZk().subscribeForTopologyChanges(() -> addTask(this::onTopologyChanged));
            } catch (final Exception e) {
                switchState(new CleanupState(e));
                return;
            }
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
        reactOnTopologyChange();
    }

    private void reactOnTopologyChange() throws NakadiRuntimeException {
        final ZkSubscriptionClient.Topology topology = topologyListener.getData();

        // Collect current partitions state from Zk
        final Map<EventTypePartition, Partition> partitions = new HashMap<>();
        Stream.of(topology.getPartitions())
                .filter(p -> getSessionId().equals(p.getSession()))
                .forEach(p -> partitions.put(p.getKey(), p));

        // Select which partitions need to be freed from this session
        final Set<EventTypePartition> freeRightNow = new HashSet<>();
        final Set<EventTypePartition> addListeners = new HashSet<>();
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

    private void registerListener(final EventTypePartition key) {
        listeners.put(
                key,
                getZk().subscribeForOffsetChanges(
                        key, () -> addTask(() -> this.reactOnOffset(key))));
        reactOnOffset(key);
    }

    private void reactOnOffset(final EventTypePartition key) {
        if (!listeners.containsKey(key)) {
            return;
        }
        final NakadiCursor newCursor;
        try {
            newCursor = getContext().getCursorConverter().convert(key.getEventType(), listeners.get(key).getData());
        } catch (Exception ex) {
            throw new NakadiRuntimeException(ex);
        }
        if (uncommittedOffsets.containsKey(key) &&
                getComparator().compare(uncommittedOffsets.get(key), newCursor) <= 0) {
            getAutocommit().onCommit(newCursor);
            getAutocommit().autocommit();
            freePartitions(Collections.singletonList(key));
        }
        tryCompleteState();
    }

    private void tryCompleteState() {
        if (uncommittedOffsets.isEmpty()) {
            switchState(new CleanupState());
        }
    }

    private void freePartitions(final Collection<EventTypePartition> keys) {
        RuntimeException exceptionCaught = null;
        for (final EventTypePartition partitionKey : keys) {
            uncommittedOffsets.remove(partitionKey);
            final ZkSubscription<SubscriptionCursorWithoutToken> listener = listeners.remove(partitionKey);
            if (null != listener) {
                try {
                    listener.close();
                } catch (final RuntimeException ex) {
                    exceptionCaught = ex;
                    LOG.error("Failed to cancel offsets listener {}", listener, ex);
                }
            }
        }
        getZk().transfer(getSessionId(), keys);
        if (null != exceptionCaught) {
            throw exceptionCaught;
        }
    }
}
