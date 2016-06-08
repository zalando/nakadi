package de.zalando.aruha.nakadi.service.subscription.state;

import de.zalando.aruha.nakadi.service.EventStream;
import de.zalando.aruha.nakadi.service.subscription.StreamingContext;
import de.zalando.aruha.nakadi.service.subscription.zk.ZkSubscriptionClient;
import de.zalando.aruha.nakadi.service.subscription.model.Partition;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.SortedMap;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class StreamingState extends State {
    private ZkSubscriptionClient.ZKSubscription topologyChangeSubscription;
    private Consumer<String, String> kafkaConsumer;
    private final Map<Partition.PartitionKey, PartitionData> offsets = new HashMap<>();
    // Maps partition barrier when releasing must be completed or stream will be closed.
    // The reasons for that if there are two partitions (p0, p1) and p0 is reassigned, if p1 is working
    // correctly, and p0 is not receiving any updates - reassignment won't complete.
    private final Map<Partition.PartitionKey, Long> releasingPartitions = new HashMap<>();
    private boolean pollPaused = false;
    private long lastCommitMillis;
    private long commitedEvents = 0;
    private long sentEvents = 0;

    private static final Logger LOG = LoggerFactory.getLogger(StreamingState.class);

    StreamingState(final StreamingContext context) {
        super(context);
    }

    @Override
    public void onEnter() {
        // Create kafka consumer
        this.kafkaConsumer = context.kafkaClient.createKafkaConsumer();
        // Subscribe for topology changes.
        this.topologyChangeSubscription = context.zkClient.subscribeForTopologyChanges(() -> context.addTask(this::topologyChanged));
        // and call directly
        topologyChanged();
        this.context.addTask(this::pollDataFromKafka);
        this.context.scheduleTask(this::checkBatchTimeouts, context.parameters.batchTimeoutMillis, TimeUnit.MILLISECONDS);

        if (null != context.parameters.streamTimeoutMillis) {
            this.context.scheduleTask(this::shutdownGracefully, context.parameters.streamTimeoutMillis, TimeUnit.MILLISECONDS);
        }

        this.lastCommitMillis = System.currentTimeMillis();
        this.context.scheduleTask(this::checkCommitTimeout, context.parameters.commitTimeoutMillis, TimeUnit.MILLISECONDS);
    }

    private void checkCommitTimeout() {
        if (!isCurrent()) {
            return;
        }
        final long currentMillis = System.currentTimeMillis();
        final boolean hasUncommited = offsets.values().stream().filter(d -> !d.isCommited()).findAny().isPresent();
        if (hasUncommited) {
            final long millisFromLastCommit = currentMillis - lastCommitMillis;
            if (millisFromLastCommit >= context.parameters.commitTimeoutMillis) {
                LOG.info("Commit timeout reached, shutting down");
                shutdownGracefully();
            } else {
                context.scheduleTask(this::checkCommitTimeout, context.parameters.commitTimeoutMillis - millisFromLastCommit, TimeUnit.MILLISECONDS);
            }
        } else {
            context.scheduleTask(this::checkCommitTimeout, context.parameters.commitTimeoutMillis, TimeUnit.MILLISECONDS);
        }
    }

    private void shutdownGracefully() {
        if (!isCurrent()) {
            return;
        }
        final Map<Partition.PartitionKey, Long> uncommited = offsets.entrySet().stream()
                .filter(e -> !e.getValue().isCommited())
                .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().getSentOffset()));
        if (uncommited.isEmpty()) {
            context.switchState(new CleanupState(context));
        } else {
            context.switchState(new ClosingState(context, uncommited, lastCommitMillis));
        }
    }

    private void pollDataFromKafka() {
        if (!isCurrent() || null == kafkaConsumer) {
            LOG.info("Poll process from kafka is stopped, because state is switched or switching");
            return;
        }
        if (kafkaConsumer.assignment().isEmpty() || pollPaused) {
            // Small optimization not to waste CPU while not yet assigned to any partitions
            this.context.scheduleTask(this::pollDataFromKafka, context.kafkaPollTimeout, TimeUnit.MILLISECONDS);
            return;
        }
        final ConsumerRecords<String, String> records = kafkaConsumer.poll(context.kafkaPollTimeout);
        if (!records.isEmpty()) {
            for (final TopicPartition tp : records.partitions()) {
                final Partition.PartitionKey pk = new Partition.PartitionKey(tp.topic(), String.valueOf(tp.partition()));
                final PartitionData pd = offsets.get(pk);
                if (null != pd) {
                    for (final ConsumerRecord<String, String> record : records.records(tp)) {
                        pd.addEvent(record.offset(), record.value());
                    }
                }
            }
            this.context.addTask(this::streamToOutput);
        }
        // Yep, no timeout. All waits are in kafka.
        // It works because only one pollDataFromKafka task is present in queue each time. Poll process will stop when this state
        // will be changed to any other state.
        this.context.addTask(this::pollDataFromKafka);
    }

    private long getMessagesAllowedToSend() {
        final long unconfirmed = offsets.values().stream().mapToLong(PartitionData::getUnconfirmed).sum();
        final long allowDueWindowSize = context.parameters.windowSizeMessages - unconfirmed;
        if (null == context.parameters.streamLimitEvents) {
            return allowDueWindowSize;
        }
        return Math.min(allowDueWindowSize, context.parameters.streamLimitEvents - this.sentEvents);
    }

    private void checkBatchTimeouts() {
        if (!isCurrent()) {
            return;
        }
        streamToOutput();
        final OptionalLong lastSent = offsets.values().stream().mapToLong(PartitionData::getLastSendMillis).min();
        final long nextCall = lastSent.orElse(System.currentTimeMillis()) + context.parameters.batchTimeoutMillis;
        final long delta = nextCall - System.currentTimeMillis();
        if (delta > 0) {
            context.scheduleTask(this::checkBatchTimeouts, delta, TimeUnit.MILLISECONDS);
        } else {
            LOG.debug("Probably acting too slow, stream timeouts are constantly rescheduled");
            context.addTask(this::checkBatchTimeouts);
        }
    }

    private void streamToOutput() {
        if (!isCurrent()) {
            return;
        }
        final long currentTimeMillis = System.currentTimeMillis();
        int freeSlots = (int) getMessagesAllowedToSend();
        SortedMap<Long, String> toSend;
        for (final Map.Entry<Partition.PartitionKey, PartitionData> e : offsets.entrySet()) {
            while (null != (toSend = e.getValue().takeEventsToStream(
                    currentTimeMillis,
                    Math.min(context.parameters.batchLimitEvents, freeSlots),
                    context.parameters.batchTimeoutMillis))) {
                flushData(e.getKey(), toSend);
                this.sentEvents += toSend.size();
                if (toSend.isEmpty()) {
                    break;
                }
                freeSlots -= toSend.size();
            }
        }
        pollPaused = getMessagesAllowedToSend() <= 0;
        if (null != context.parameters.batchKeepAliveIterations) {
            final boolean allPartitionsReachedKeepAliveLimit = offsets.values().stream()
                    .map(PartitionData::getKeepAlivesInARow)
                    .allMatch(v -> v >= context.parameters.batchKeepAliveIterations);
            if (allPartitionsReachedKeepAliveLimit) {
                shutdownGracefully();
            }
        }
    }

    private void flushData(final Partition.PartitionKey pk, final SortedMap<Long, String> data) {
        final String evt = EventStream.createStreamEvent(
                pk.partition,
                String.valueOf(offsets.get(pk).getSentOffset()),
                new ArrayList<>(data.values()),
                Optional.empty());
        try {
            context.out.streamData(evt.getBytes(EventStream.UTF8));
        } catch (final IOException e) {
            LOG.error("Failed to write data to output. Session: " + context.session, e);
            shutdownGracefully();
        }
    }

    @Override
    public void onExit() {
        if (null != topologyChangeSubscription) {
            try {
                topologyChangeSubscription.cancel();
            } catch (final RuntimeException ex) {
                LOG.warn("Failed to cancel topology subscription", ex);
            } finally {
                topologyChangeSubscription = null;
                new HashSet<>(offsets.keySet()).forEach(this::removeFromStreaming);
            }
        }
        if (null != kafkaConsumer) {
            try {
                kafkaConsumer.close();
            } finally {
                kafkaConsumer = null;
            }
        }
    }

    private void topologyChanged() {
        if (!isCurrent()) {
            return;
        }
        context.zkClient.lock(() -> {
            final Partition[] assignedPartitions = Stream.of(context.zkClient.listPartitions())
                    .filter(p -> context.session.id.equals(p.getSession()))
                    .toArray(Partition[]::new);
            context.addTask(() -> refreshTopologyUnlocked(assignedPartitions));
        });
    }

    private void refreshTopologyUnlocked(final Partition[] assignedPartitions) {
        if (!isCurrent()) {
            return;
        }
        final Map<Partition.PartitionKey, Partition> newAssigned = Stream.of(assignedPartitions)
                .filter(p -> p.getState() == Partition.State.ASSIGNED)
                .collect(Collectors.toMap(Partition::getKey, p -> p));

        final Map<Partition.PartitionKey, Partition> newReassigning = Stream.of(assignedPartitions)
                .filter(p -> p.getState() == Partition.State.REASSIGNING)
                .collect(Collectors.toMap(Partition::getKey, p -> p));

        // 1. Select which partitions must be removed right now for some (strange) reasons (no need to release topology).
        offsets.keySet().stream()
                .filter(e -> !newAssigned.containsKey(e))
                .filter(e -> !newReassigning.containsKey(e))
                .collect(Collectors.toList()).forEach(this::removeFromStreaming);
        // 2. Clear releasing partitions that was returned to this session.
        releasingPartitions.keySet().stream()
                .filter(p -> !newReassigning.containsKey(p))
                .collect(Collectors.toList()).forEach(releasingPartitions::remove);

        // 3. Add releasing partitions information
        if (!newReassigning.isEmpty()) {
            newReassigning.keySet().forEach(this::addPartitionToReassigned);
        }

        // 4. Select which partitions must be added and add it.
        newAssigned.values()
                .stream()
                .filter(p -> !offsets.containsKey(p.getKey()))
                .forEach(this::addToStreaming);
        // 5. Check if something can be released right now
        reassignCommited();
        // 6. Reconfigure kafka consumer
        reconfigureKafkaConsumer(false);
    }

    private void addPartitionToReassigned(final Partition.PartitionKey partitionKey) {
        final long currentTime = System.currentTimeMillis();
        final long barrier = currentTime + context.parameters.commitTimeoutMillis;
        releasingPartitions.put(partitionKey, barrier);
        context.scheduleTask(() -> barrierOnRebalanceReached(partitionKey), context.parameters.commitTimeoutMillis, TimeUnit.MILLISECONDS);
    }

    private void barrierOnRebalanceReached(final Partition.PartitionKey pk) {
        if (!isCurrent() || !releasingPartitions.containsKey(pk)) {
            return;
        }
        final long currentTime = System.currentTimeMillis();
        if (currentTime >= releasingPartitions.get(pk)) {
            shutdownGracefully();
        } else {
            // Schedule again, probably something happened (ex. rebalance twice)
            context.scheduleTask(() -> barrierOnRebalanceReached(pk), releasingPartitions.get(pk) - currentTime, TimeUnit.MILLISECONDS);
        }
    }

    private void reconfigureKafkaConsumer(final boolean forceSeek) {
        final Set<Partition.PartitionKey> currentAssignment = kafkaConsumer.assignment().stream()
                .map(tp -> new Partition.PartitionKey(tp.topic(), String.valueOf(tp.partition())))
                .collect(Collectors.toSet());
        if (!currentAssignment.equals(offsets.keySet()) || forceSeek) {
            final Map<Partition.PartitionKey, TopicPartition> kafkaKeys = offsets.keySet().stream().collect(
                    Collectors.toMap(
                            k -> k,
                            k -> new TopicPartition(k.topic, Integer.valueOf(k.partition))));
            // Ignore order
            kafkaConsumer.assign(new ArrayList<>(kafkaKeys.values()));
            kafkaKeys.forEach((k, v) -> kafkaConsumer.seek(v, offsets.get(k).getSentOffset()));
            offsets.values().forEach(PartitionData::clearEvents);
        }
    }

    private void addToStreaming(final Partition partition) {
        offsets.put(partition.getKey(), new PartitionData(
                context.zkClient.subscribeForOffsetChanges(partition.getKey(), o -> offsetChanged(partition.getKey(), o)),
                context.zkClient.getOffset(partition.getKey())));
    }

    private void reassignCommited() {
        final List<Partition.PartitionKey> keysToRelease = releasingPartitions.keySet().stream()
                .filter(pk -> !offsets.containsKey(pk) || offsets.get(pk).isCommited())
                .collect(Collectors.toList());
        if (!keysToRelease.isEmpty()) {
            try {
                keysToRelease.forEach(this::removeFromStreaming);
            } finally {
                context.zkClient.lock(() -> context.zkClient.transfer(context.session.id, keysToRelease));
            }
        }
    }

    private void offsetChanged(final Partition.PartitionKey key, final Long offset) {
        context.addTask(() -> {
            if (offsets.containsKey(key)) {
                final PartitionData data = offsets.get(key);
                final PartitionData.CommitResult commitResult = data.onCommitOffset(offset);
                if (commitResult.seekOnKafka) {
                    reconfigureKafkaConsumer(true);
                }
                if (commitResult.commitedCount > 0) {
                    commitedEvents += commitResult.commitedCount;
                    this.lastCommitMillis = System.currentTimeMillis();
                }
                if (null != context.parameters.streamLimitEvents && context.parameters.streamLimitEvents <= commitedEvents) {
                    shutdownGracefully();
                }
                if (releasingPartitions.containsKey(key) && data.isCommited()) {
                    reassignCommited();
                }
            }
        });
    }

    private void removeFromStreaming(final Partition.PartitionKey key) {
        LOG.info("Removing completely from streaming: " + key);
        releasingPartitions.remove(key);
        final PartitionData data = offsets.remove(key);
        if (null != data) {
            try {
                if (data.getUnconfirmed() > 0) {
                    LOG.warn("Skipping commits: " + key + ", commit=" + (data.getSentOffset() - data.getUnconfirmed()) + ", sent=" + data.getSentOffset());
                }
                data.getSubscription().cancel();
            } catch (final RuntimeException ex) {
                LOG.warn("Failed to cancel subscription, skipping exception", ex);
            }
        }
    }
}
