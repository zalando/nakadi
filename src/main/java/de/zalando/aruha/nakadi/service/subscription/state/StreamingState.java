package de.zalando.aruha.nakadi.service.subscription.state;

import de.zalando.aruha.nakadi.service.EventStream;
import de.zalando.aruha.nakadi.service.subscription.model.Partition;
import de.zalando.aruha.nakadi.service.subscription.zk.ZKSubscription;
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
import org.slf4j.LoggerFactory;

class StreamingState extends State {
    private ZKSubscription topologyChangeSubscription;
    private Consumer<String, String> kafkaConsumer;
    private final Map<Partition.PartitionKey, PartitionData> offsets = new HashMap<>();
    // Maps partition barrier when releasing must be completed or stream will be closed.
    // The reasons for that if there are two partitions (p0, p1) and p0 is reassigned, if p1 is working
    // correctly, and p0 is not receiving any updates - reassignment won't complete.
    private final Map<Partition.PartitionKey, Long> releasingPartitions = new HashMap<>();
    private boolean pollPaused = false;
    private long lastCommitMillis;
    private long committedEvents = 0;
    private long sentEvents = 0;

    @Override
    public void onEnter() {
        // Create kafka consumer
        this.kafkaConsumer = getKafka().createKafkaConsumer();
        // Subscribe for topology changes.
        this.topologyChangeSubscription = getZk().subscribeForTopologyChanges(() -> addTask(this::topologyChanged));
        // and call directly
        reactOnTopologyChange();
        addTask(this::pollDataFromKafka);
        scheduleTask(this::checkBatchTimeouts, getParameters().batchTimeoutMillis, TimeUnit.MILLISECONDS);

        getParameters().streamTimeoutMillis.ifPresent(
                timeout -> scheduleTask(() ->this.shutdownGracefully("Stream timeout reached"), timeout, TimeUnit.MILLISECONDS));

        this.lastCommitMillis = System.currentTimeMillis();
        scheduleTask(this::checkCommitTimeout, getParameters().commitTimeoutMillis, TimeUnit.MILLISECONDS);
    }

    private void checkCommitTimeout() {
        final long currentMillis = System.currentTimeMillis();
        final boolean hasUncommitted = offsets.values().stream().filter(d -> !d.isCommitted()).findAny().isPresent();
        if (hasUncommitted) {
            final long millisFromLastCommit = currentMillis - lastCommitMillis;
            if (millisFromLastCommit >= getParameters().commitTimeoutMillis) {
                shutdownGracefully("Commit timeout reached");
            } else {
                scheduleTask(this::checkCommitTimeout, getParameters().commitTimeoutMillis - millisFromLastCommit, TimeUnit.MILLISECONDS);
            }
        } else {
            scheduleTask(this::checkCommitTimeout, getParameters().commitTimeoutMillis, TimeUnit.MILLISECONDS);
        }
    }

    private void shutdownGracefully(final String reason) {
        getLog().info("Shutting down gracefully. Reason: {}", reason);
        final Map<Partition.PartitionKey, Long> uncommitted = offsets.entrySet().stream()
                .filter(e -> !e.getValue().isCommitted())
                .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().getSentOffset()));
        if (uncommitted.isEmpty()) {
            switchState(new CleanupState());
        } else {
            switchState(new ClosingState(uncommitted, lastCommitMillis));
        }
    }

    private void pollDataFromKafka() {
        if (kafkaConsumer.assignment().isEmpty() || pollPaused) {
            // Small optimization not to waste CPU while not yet assigned to any partitions
            scheduleTask(this::pollDataFromKafka, getKafkaPollTimeout(), TimeUnit.MILLISECONDS);
            return;
        }
        final ConsumerRecords<String, String> records = kafkaConsumer.poll(getKafkaPollTimeout());
        if (!records.isEmpty()) {
            for (final TopicPartition tp : records.partitions()) {
                final Partition.PartitionKey pk = new Partition.PartitionKey(tp.topic(), String.valueOf(tp.partition()));
                final PartitionData pd = offsets.get(pk);
                if (null != pd) {
                    for (final ConsumerRecord<String, String> record : records.records(tp)) {
                        pd.addEventFromKafka(record.offset(), record.value());
                    }
                }
            }
            addTask(this::streamToOutput);
        }
        // Yep, no timeout. All waits are in kafka.
        // It works because only one pollDataFromKafka task is present in queue each time. Poll process will stop
        // when this state will be changed to any other state.
        addTask(this::pollDataFromKafka);
    }

    private long getMessagesAllowedToSend() {
        final long unconfirmed = offsets.values().stream().mapToLong(PartitionData::getUnconfirmed).sum();
        final long allowDueWindowSize = getParameters().windowSizeMessages - unconfirmed;
        return getParameters().getMessagesAllowedToSend(allowDueWindowSize, this.sentEvents);
    }

    private void checkBatchTimeouts() {
        streamToOutput();
        final OptionalLong lastSent = offsets.values().stream().mapToLong(PartitionData::getLastSendMillis).min();
        final long nextCall = lastSent.orElse(System.currentTimeMillis()) + getParameters().batchTimeoutMillis;
        final long delta = nextCall - System.currentTimeMillis();
        if (delta > 0) {
            scheduleTask(this::checkBatchTimeouts, delta, TimeUnit.MILLISECONDS);
        } else {
            getLog().debug("Probably acting too slow, stream timeouts are constantly rescheduled");
            addTask(this::checkBatchTimeouts);
        }
    }

    private void streamToOutput() {
        final long currentTimeMillis = System.currentTimeMillis();
        int freeSlots = (int) getMessagesAllowedToSend();
        SortedMap<Long, String> toSend;
        for (final Map.Entry<Partition.PartitionKey, PartitionData> e : offsets.entrySet()) {
            while (null != (toSend = e.getValue().takeEventsToStream(
                    currentTimeMillis,
                    Math.min(getParameters().batchLimitEvents, freeSlots),
                    getParameters().batchTimeoutMillis))) {
                flushData(e.getKey(), toSend);
                this.sentEvents += toSend.size();
                if (toSend.isEmpty()) {
                    break;
                }
                freeSlots -= toSend.size();
            }
        }
        pollPaused = getMessagesAllowedToSend() <= 0;
        if (!offsets.isEmpty() &&
                getParameters().isKeepAliveLimitReached(offsets.values().stream().mapToInt(PartitionData::getKeepAliveInARow))) {
            shutdownGracefully("All partitions reached keepAlive limit");
        }
    }

    private void flushData(final Partition.PartitionKey pk, final SortedMap<Long, String> data) {
        final String evt = EventStream.createStreamEvent(
                pk.partition,
                String.valueOf(offsets.get(pk).getSentOffset()),
                new ArrayList<>(data.values()),
                Optional.empty());
        try {
            getOut().streamData(evt.getBytes(EventStream.UTF8));
        } catch (final IOException e) {
            getLog().error("Failed to write data to output.", e);
            shutdownGracefully("Failed to write data to output");
        }
    }

    @Override
    public void onExit() {
        if (null != topologyChangeSubscription) {
            try {
                topologyChangeSubscription.cancel();
            } catch (final RuntimeException ex) {
                getLog().warn("Failed to cancel topology subscription", ex);
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

    void topologyChanged() {
        if (null != topologyChangeSubscription) {
            topologyChangeSubscription.refresh();
        }
        reactOnTopologyChange();
    }

    private void reactOnTopologyChange() {
        getZk().runLocked(() -> {
            final Partition[] assignedPartitions = Stream.of(getZk().listPartitions())
                    .filter(p -> getSessionId().equals(p.getSession()))
                    .toArray(Partition[]::new);
            addTask(() -> refreshTopologyUnlocked(assignedPartitions));
        });
    }

    void refreshTopologyUnlocked(final Partition[] assignedPartitions) {
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
        reassignCommitted();

        // 6. Reconfigure kafka consumer
        reconfigureKafkaConsumer(false);

        logPartitionAssignment("Topology refreshed");
    }

    private void logPartitionAssignment(final String reason) {
        if (getLog().isInfoEnabled()) {
            getLog().info("{}. Streaming partitions: [{}]. Reassigning partitions: [{}]",
                    reason,
                    offsets.keySet().stream().filter(p -> !releasingPartitions.containsKey(p))
                            .map(Partition.PartitionKey::toString).collect(Collectors.joining(",")),
                    releasingPartitions.keySet().stream().map(Partition.PartitionKey::toString)
                            .collect(Collectors.joining(", ")));
        }
    }

    private void addPartitionToReassigned(final Partition.PartitionKey partitionKey) {
        final long currentTime = System.currentTimeMillis();
        final long barrier = currentTime + getParameters().commitTimeoutMillis;
        releasingPartitions.put(partitionKey, barrier);
        scheduleTask(() -> barrierOnRebalanceReached(partitionKey), getParameters().commitTimeoutMillis, TimeUnit.MILLISECONDS);
    }

    private void barrierOnRebalanceReached(final Partition.PartitionKey pk) {
        if (!releasingPartitions.containsKey(pk)) {
            return;
        }
        getLog().info("Checking barrier to transfer partition {}", pk);
        final long currentTime = System.currentTimeMillis();
        if (currentTime >= releasingPartitions.get(pk)) {
            shutdownGracefully("barrier on reassigning partition reached for " + pk + ", current time: " + currentTime + ", barrier: " + releasingPartitions.get(pk));
        } else {
            // Schedule again, probably something happened (ex. rebalance twice)
            scheduleTask(() -> barrierOnRebalanceReached(pk), releasingPartitions.get(pk) - currentTime, TimeUnit.MILLISECONDS);
        }
    }

    private void reconfigureKafkaConsumer(final boolean forceSeek) {
        final Set<Partition.PartitionKey> currentKafkaAssignment = kafkaConsumer.assignment().stream()
                .map(tp -> new Partition.PartitionKey(tp.topic(), String.valueOf(tp.partition())))
                .collect(Collectors.toSet());

        final Set<Partition.PartitionKey> currentNakadiAssignment = offsets.keySet().stream()
                .filter(o -> !this.releasingPartitions.containsKey(o))
                .collect(Collectors.toSet());
        if (!currentKafkaAssignment.equals(currentNakadiAssignment) || forceSeek) {
            final Map<Partition.PartitionKey, TopicPartition> kafkaKeys = currentNakadiAssignment.stream().collect(
                    Collectors.toMap(
                            k -> k,
                            k -> new TopicPartition(k.topic, Integer.valueOf(k.partition))));
            // Ignore order
            kafkaConsumer.assign(new ArrayList<>(kafkaKeys.values()));
            // Check if offsets are available in kafka
            kafkaConsumer.seekToBeginning(kafkaKeys.values().toArray(new TopicPartition[kafkaKeys.size()]));
            kafkaKeys.forEach((key, kafka) -> offsets.get(key).ensureDataAvailable(kafkaConsumer.position(kafka)));
            //
            kafkaKeys.forEach((k, v) -> kafkaConsumer.seek(v, offsets.get(k).getSentOffset() + 1));
            offsets.values().forEach(PartitionData::clearEvents);
        }
    }

    private void addToStreaming(final Partition partition) {
        offsets.put(
                partition.getKey(),
                new PartitionData(
                        getZk().subscribeForOffsetChanges(partition.getKey(), () -> addTask(() -> offsetChanged(partition.getKey()))),
                        getZk().getOffset(partition.getKey()),
                        LoggerFactory.getLogger("subscription." + getSessionId() + "." + partition.getKey())));
    }

    private void reassignCommitted() {
        final List<Partition.PartitionKey> keysToRelease = releasingPartitions.keySet().stream()
                .filter(pk -> !offsets.containsKey(pk) || offsets.get(pk).isCommitted())
                .collect(Collectors.toList());
        if (!keysToRelease.isEmpty()) {
            try {
                keysToRelease.forEach(this::removeFromStreaming);
            } finally {
                getZk().runLocked(() -> getZk().transfer(getSessionId(), keysToRelease));
            }
        }
    }

    void offsetChanged(final Partition.PartitionKey key) {
        if (offsets.containsKey(key)) {
            final PartitionData data = offsets.get(key);
            data.getSubscription().refresh();
            final long offset = getZk().getOffset(key);
            final PartitionData.CommitResult commitResult = data.onCommitOffset(offset);
            if (commitResult.seekOnKafka) {
                reconfigureKafkaConsumer(true);
            }
            if (commitResult.committedCount > 0) {
                committedEvents += commitResult.committedCount;
                this.lastCommitMillis = System.currentTimeMillis();
                streamToOutput();
            }
            if (getParameters().isStreamLimitReached(committedEvents)) {
                shutdownGracefully("Stream limit in events reached: " + committedEvents);
            }
            if (releasingPartitions.containsKey(key) && data.isCommitted()) {
                reassignCommitted();
                logPartitionAssignment("New offset received for releasing partition " + key);
            }
        }
    }

    private void removeFromStreaming(final Partition.PartitionKey key) {
        getLog().info("Removing partition {} from streaming", key);
        releasingPartitions.remove(key);
        final PartitionData data = offsets.remove(key);
        if (null != data) {
            try {
                if (data.getUnconfirmed() > 0) {
                    getLog().warn("Skipping commits: {}, commit={}, sent={}", key, data.getSentOffset() - data.getUnconfirmed(), data.getSentOffset());
                }
                data.getSubscription().cancel();
            } catch (final RuntimeException ex) {
                getLog().warn("Failed to cancel subscription, skipping exception", ex);
            }
        }
    }
}
