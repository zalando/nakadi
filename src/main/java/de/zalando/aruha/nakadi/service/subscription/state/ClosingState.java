package de.zalando.aruha.nakadi.service.subscription.state;

import de.zalando.aruha.nakadi.service.subscription.StreamingContext;
import de.zalando.aruha.nakadi.service.subscription.model.Partition;
import de.zalando.aruha.nakadi.service.subscription.zk.ZkSubscriptionClient;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

class ClosingState extends State {
    private final Map<Partition.PartitionKey, Long> uncommitedOffsets;
    private final Map<Partition.PartitionKey, ZkSubscriptionClient.ZKSubscription> listeners = new HashMap<>();

    ClosingState(final StreamingContext context, final Map<Partition.PartitionKey, Long> uncommitedOffsets) {
        super(context);
        this.uncommitedOffsets = uncommitedOffsets;
    }

    @Override
    public void onExit() {
        super.onExit();
    }

    @Override
    public void onEnter() {
        context.zkClient.subscribeForTopologyChanges(() -> context.addTask(this::onTopologyChanged));
        onTopologyChanged();
    }

    private void onTopologyChanged() {
        if (!isCurrent()) {
            return;
        }
        final Map<Partition.PartitionKey, Partition> partitions = new HashMap<>();
        context.zkClient.lock(() -> Stream.of(context.zkClient.listPartitions())
                .filter(p -> context.session.id.equals(p.getSession()))
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
        freePartition(freeRightNow);
        addListeners.forEach(this::registerListener);
        tryCompleteState();
    }

    private void registerListener(final Partition.PartitionKey key) {
        listeners.put(
                key,
                context.zkClient.subscribeForOffsetChanges(
                        key, newOffset -> context.addTask(() -> this.reactOnOffset(key, newOffset))));
        reactOnOffset(key, context.zkClient.getOffset(key));
    }

    private void reactOnOffset(final Partition.PartitionKey key, final Long newOffset) {
        if (!isCurrent()) {
            return;
        }
        if (uncommitedOffsets.containsKey(key) && uncommitedOffsets.get(key) <= newOffset) {
            freePartition(Collections.singletonList(key));
        }
        tryCompleteState();
    }

    private void tryCompleteState() {
        if (uncommitedOffsets.isEmpty()) {
            context.switchState(new CleanupState(context));
        }
    }

    private void freePartition(final Collection<Partition.PartitionKey> keys) {
        for (final Partition.PartitionKey partitionKey : keys) {
            uncommitedOffsets.remove(partitionKey);
            final ZkSubscriptionClient.ZKSubscription listener = listeners.remove(partitionKey);
            if (null != listener) {
                listener.cancel();
            }
        }
        context.zkClient.lock(() -> context.zkClient.transfer(context.session.id, keys));
    }
}
