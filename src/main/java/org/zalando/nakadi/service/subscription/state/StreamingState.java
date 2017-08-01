package org.zalando.nakadi.service.subscription.state;

import com.codahale.metrics.Meter;
import com.google.common.base.Preconditions;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.slf4j.LoggerFactory;
import org.zalando.nakadi.domain.ConsumedEvent;
import org.zalando.nakadi.domain.EventTypePartition;
import org.zalando.nakadi.domain.NakadiCursor;
import org.zalando.nakadi.domain.PartitionStatistics;
import org.zalando.nakadi.domain.Timeline;
import org.zalando.nakadi.exceptions.InternalNakadiException;
import org.zalando.nakadi.exceptions.InvalidCursorException;
import org.zalando.nakadi.exceptions.NakadiException;
import org.zalando.nakadi.exceptions.NakadiRuntimeException;
import org.zalando.nakadi.exceptions.NoSuchEventTypeException;
import org.zalando.nakadi.exceptions.ServiceUnavailableException;
import org.zalando.nakadi.metrics.MetricUtils;
import org.zalando.nakadi.repository.EventConsumer;
import org.zalando.nakadi.service.subscription.model.Partition;
import org.zalando.nakadi.service.subscription.zk.ZKSubscription;
import org.zalando.nakadi.view.SubscriptionCursor;
import org.zalando.nakadi.view.SubscriptionCursorWithoutToken;


class StreamingState extends State {
    private final Map<EventTypePartition, PartitionData> offsets = new HashMap<>();
    // Maps partition barrier when releasing must be completed or stream will be closed.
    // The reasons for that if there are two partitions (p0, p1) and p0 is reassigned, if p1 is working
    // correctly, and p0 is not receiving any updates - reassignment won't complete.
    private final Map<EventTypePartition, Long> releasingPartitions = new HashMap<>();
    private ZKSubscription topologyChangeSubscription;
    private EventConsumer.ReassignableEventConsumer eventConsumer;
    private boolean pollPaused;
    private long committedEvents;
    private long sentEvents;
    private long batchesSent;
    private Meter bytesSentMeter;
    // Uncommitted offsets are calculated right on exiting from Streaming state.
    private Map<EventTypePartition, NakadiCursor> uncommittedOffsets;
    private ZKSubscription cursorResetSubscription;

    /**
     * Time that is used for commit timeout check. Commit timeout check is working only in case when there is something
     * uncommitted. Value is updated in 2 cases: <ul>
     * <li>something was committed</li>
     * <li>stream moves from state when everything was committed to state with uncommitted events</li>
     * </ul>
     */
    private long lastCommitMillis;

    @Override
    public void onEnter() {
        final String kafkaFlushedBytesMetricName = MetricUtils.metricNameForHiLAStream(
                this.getContext().getParameters().getConsumingAppId(),
                this.getContext().getSubscription().getId()
        );
        bytesSentMeter = this.getContext().getMetricRegistry().meter(kafkaFlushedBytesMetricName);

        this.eventConsumer = getContext().getTimelineService().createEventConsumer(null);

        // Subscribe for topology changes.
        this.topologyChangeSubscription = getZk().subscribeForTopologyChanges(() -> addTask(this::topologyChanged));
        // and call directly
        reactOnTopologyChange();
        addTask(this::pollDataFromKafka);
        scheduleTask(this::checkBatchTimeouts, getParameters().batchTimeoutMillis, TimeUnit.MILLISECONDS);

        getParameters().streamTimeoutMillis.ifPresent(
                timeout -> scheduleTask(() -> {
                            final String debugMessage = "Stream timeout reached";
                            this.sendMetadata(debugMessage);
                            this.shutdownGracefully(debugMessage);
                        }, timeout,
                        TimeUnit.MILLISECONDS));

        this.lastCommitMillis = System.currentTimeMillis();
        scheduleTask(this::checkCommitTimeout, getParameters().commitTimeoutMillis, TimeUnit.MILLISECONDS);

        cursorResetSubscription = getZk().subscribeForCursorsReset(
                () -> addTask(this::resetSubscriptionCursorsCallback));
    }

    private void resetSubscriptionCursorsCallback() {
        final String message = "Resetting subscription cursors";
        sendMetadata(message);
        shutdownGracefully(message);
    }

    private void checkCommitTimeout() {
        final long currentMillis = System.currentTimeMillis();
        if (!isEverythingCommitted()) {
            final long millisFromLastCommit = currentMillis - lastCommitMillis;
            if (millisFromLastCommit >= getParameters().commitTimeoutMillis) {
                final String debugMessage = "Commit timeout reached";
                sendMetadata(debugMessage);
                shutdownGracefully(debugMessage);
            } else {
                scheduleTask(this::checkCommitTimeout, getParameters().commitTimeoutMillis - millisFromLastCommit,
                        TimeUnit.MILLISECONDS);
            }
        } else {
            scheduleTask(this::checkCommitTimeout, getParameters().commitTimeoutMillis, TimeUnit.MILLISECONDS);
        }
    }

    private boolean isEverythingCommitted() {
        return offsets.values().stream().allMatch(PartitionData::isCommitted);
    }

    private void sendMetadata(final String metadata) {
        offsets.entrySet().stream().findFirst()
                .ifPresent(pk -> flushData(pk.getKey(), Collections.emptyList(), Optional.of(metadata)));
    }

    private long getLastCommitMillis() {
        return lastCommitMillis;
    }

    private Map<EventTypePartition, NakadiCursor> getUncommittedOffsets() {
        Preconditions.checkNotNull(uncommittedOffsets, "uncommittedOffsets should not be null on time of call");
        return uncommittedOffsets;
    }

    private void shutdownGracefully(final String reason) {
        getLog().info("Shutting down gracefully. Reason: {}", reason);
        switchState(new ClosingState(this::getUncommittedOffsets, this::getLastCommitMillis));
    }

    private void pollDataFromKafka() {
        if (eventConsumer == null) {
            throw new IllegalStateException("kafkaConsumer should not be null when calling pollDataFromKafka method");
        }

        if (!isConnectionReady()) {
            shutdownGracefully("Hila connection closed via crutch");
            return;
        }

        if (isSubscriptionConsumptionBlocked()) {
            final String message = "Consumption is blocked";
            sendMetadata(message);
            shutdownGracefully(message);
            return;
        }

        if (eventConsumer.getAssignment().isEmpty() || pollPaused) {
            // Small optimization not to waste CPU while not yet assigned to any partitions
            scheduleTask(this::pollDataFromKafka, getKafkaPollTimeout(), TimeUnit.MILLISECONDS);
            return;
        }
        final List<ConsumedEvent> events = eventConsumer.readEvents();
        events.forEach(this::rememberEvent);
        if (!events.isEmpty()) {
            addTask(this::streamToOutput);
        }

        // Yep, no timeout. All waits are in kafka.
        // It works because only one pollDataFromKafka task is present in queue each time. Poll process will stop
        // when this state will be changed to any other state.
        addTask(this::pollDataFromKafka);
    }

    private void rememberEvent(final ConsumedEvent event) {
        Optional.ofNullable(offsets.get(event.getPosition().getEventTypePartition()))
                .ifPresent(pd -> pd.addEvent(event));
    }

    private long getMessagesAllowedToSend() {
        final long unconfirmed = offsets.values().stream().mapToLong(PartitionData::getUnconfirmed).sum();
        final long limit = getParameters().maxUncommittedMessages - unconfirmed;
        return getParameters().getMessagesAllowedToSend(limit, this.sentEvents);
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
        int messagesAllowedToSend = (int) getMessagesAllowedToSend();
        final boolean wasCommitted = isEverythingCommitted();
        boolean sentSomething = false;

        for (final Map.Entry<EventTypePartition, PartitionData> e : offsets.entrySet()) {
            List<ConsumedEvent> toSend;
            while (null != (toSend = e.getValue().takeEventsToStream(
                    currentTimeMillis,
                    Math.min(getParameters().batchLimitEvents, messagesAllowedToSend),
                    getParameters().batchTimeoutMillis))) {
                sentSomething |= !toSend.isEmpty();
                flushData(e.getKey(), toSend, batchesSent == 0 ? Optional.of("Stream started") : Optional.empty());
                this.sentEvents += toSend.size();
                if (toSend.isEmpty()) {
                    break;
                }
                messagesAllowedToSend -= toSend.size();
            }
        }
        if (wasCommitted && sentSomething) {
            this.lastCommitMillis = System.currentTimeMillis();
        }
        pollPaused = getMessagesAllowedToSend() <= 0;
        if (!offsets.isEmpty() &&
                getParameters().isKeepAliveLimitReached(offsets.values().stream()
                        .mapToInt(PartitionData::getKeepAliveInARow))) {
            shutdownGracefully("All partitions reached keepAlive limit");
        }
    }

    private void flushData(final EventTypePartition pk, final List<ConsumedEvent> data,
                           final Optional<String> metadata) {
        try {
            final NakadiCursor sentOffset = offsets.get(pk).getSentOffset();
            final SubscriptionCursor cursor = getContext().getCursorConverter().convert(
                    sentOffset,
                    getContext().getCursorTokenService().generateToken());

            final int batchSize = getContext().getWriter().writeSubscriptionBatch(
                    getOut().getOutputStream(),
                    cursor,
                    data,
                    metadata);

            bytesSentMeter.mark(batchSize);
            batchesSent++;
        } catch (final IOException e) {
            getLog().error("Failed to write data to output.", e);
            shutdownGracefully("Failed to write data to output");
        }
    }

    @Override
    public void onExit() {
        uncommittedOffsets = offsets.entrySet().stream()
                .filter(e -> !e.getValue().isCommitted())
                .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().getSentOffset()));

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
        if (null != eventConsumer) {
            try {
                eventConsumer.close();
            } catch (final IOException e) {
                throw new NakadiRuntimeException(e);
            } finally {
                eventConsumer = null;
            }
        }

        if (cursorResetSubscription != null) {
            cursorResetSubscription.cancel();
            cursorResetSubscription = null;
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
        final Map<EventTypePartition, Partition> newAssigned = Stream.of(assignedPartitions)
                .filter(p -> p.getState() == Partition.State.ASSIGNED)
                .collect(Collectors.toMap(Partition::getKey, p -> p));

        final Map<EventTypePartition, Partition> newReassigning = Stream.of(assignedPartitions)
                .filter(p -> p.getState() == Partition.State.REASSIGNING)
                .collect(Collectors.toMap(Partition::getKey, p -> p));

        // 1. Select which partitions must be removed right now for some (strange) reasons
        // (no need to release topology).
        offsets.keySet().stream()
                .filter(e -> !newAssigned.containsKey(e))
                .filter(e -> !newReassigning.containsKey(e))
                .collect(Collectors.toList()).forEach(this::removeFromStreaming);
        // 2. Clear releasing partitions that was returned to this session.
        releasingPartitions.keySet().stream()
                .filter(p -> !newReassigning.containsKey(p))
                .collect(Collectors.toList()).forEach(releasingPartitions::remove);

        // 3. Add releasing partitions information
        newReassigning.keySet().forEach(this::addPartitionToReassigned);

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
                            .map(EventTypePartition::toString).collect(Collectors.joining(",")),
                    releasingPartitions.keySet().stream().map(EventTypePartition::toString)
                            .collect(Collectors.joining(", ")));
        }
    }

    private void addPartitionToReassigned(final EventTypePartition partitionKey) {
        if (!releasingPartitions.containsKey(partitionKey)) {
            final long currentTime = System.currentTimeMillis();
            final long barrier = currentTime + getParameters().commitTimeoutMillis;
            releasingPartitions.put(partitionKey, barrier);
            scheduleTask(() -> barrierOnRebalanceReached(partitionKey), getParameters().commitTimeoutMillis,
                    TimeUnit.MILLISECONDS);
        }
    }

    private void barrierOnRebalanceReached(final EventTypePartition pk) {
        if (!releasingPartitions.containsKey(pk)) {
            return;
        }
        getLog().info("Checking barrier to transfer partition {}", pk);
        final long currentTime = System.currentTimeMillis();
        if (currentTime >= releasingPartitions.get(pk)) {
            shutdownGracefully("barrier on reassigning partition reached for " + pk + ", current time: " + currentTime
                    + ", barrier: " + releasingPartitions.get(pk));
        } else {
            // Schedule again, probably something happened (ex. rebalance twice)
            scheduleTask(() -> barrierOnRebalanceReached(pk), releasingPartitions.get(pk) - currentTime,
                    TimeUnit.MILLISECONDS);
        }
    }

    private void reconfigureKafkaConsumer(final boolean forceSeek) {
        if (eventConsumer == null) {
            throw new IllegalStateException(
                    "kafkaConsumer should not be null when calling reconfigureKafkaConsumer method");
        }
        final Set<EventTypePartition> newAssignment = offsets.keySet().stream()
                .filter(o -> !this.releasingPartitions.containsKey(o))
                .collect(Collectors.toSet());
        if (forceSeek) {
            // Force seek means that user committed somewhere not within [commitOffset, sentOffset], and one
            // should fully reconsume all the information from underlying storage. In order to do that, we are
            // removing all the current assignments for real consumer.
            try {
                eventConsumer.reassign(Collections.emptyList());
            } catch (final NakadiException | InvalidCursorException ex) {
                throw new NakadiRuntimeException(ex);
            }
        }
        final Set<EventTypePartition> currentAssignment = eventConsumer.getAssignment();

        getLog().info("Changing kafka assignment from {} to {}",
                Arrays.deepToString(currentAssignment.toArray()),
                Arrays.deepToString(newAssignment.toArray()));

        if (!currentAssignment.equals(newAssignment)) {
            try {
                final List<NakadiCursor> cursors = new ArrayList<>();
                for (final EventTypePartition pk : newAssignment) {
                    // Next 2 lines checks that current cursor is still available in storage
                    final NakadiCursor beforeFirstAvailable = getBeforeFirstCursor(pk);
                    offsets.get(pk).ensureDataAvailable(beforeFirstAvailable);
                    // Now it is safe to reposition.
                    cursors.add(offsets.get(pk).getSentOffset());
                }
                eventConsumer.reassign(cursors);
            } catch (NakadiException | InvalidCursorException ex) {
                throw new NakadiRuntimeException(ex);
            }
        }
    }

    private NakadiCursor getBeforeFirstCursor(final EventTypePartition pk)
            throws InternalNakadiException, NoSuchEventTypeException, ServiceUnavailableException {
        final Timeline firstTimelineForET = getContext().getTimelineService()
                .getActiveTimelinesOrdered(pk.getEventType()).get(0);

        final Optional<PartitionStatistics> stats = getContext().getTimelineService()
                .getTopicRepository(firstTimelineForET)
                .loadPartitionStatistics(firstTimelineForET, pk.getPartition());

        return stats.get().getBeforeFirst();
    }

    private NakadiCursor createNakadiCursor(final SubscriptionCursorWithoutToken cursor) {
        try {
            return getContext().getCursorConverter().convert(cursor);
        } catch (final Exception ex) {
            throw new NakadiRuntimeException(ex);
        }
    }

    private void addToStreaming(final Partition partition) {
        final NakadiCursor cursor = createNakadiCursor(getZk().getOffset(partition.getKey()));
        getLog().info("Adding to streaming {} with start position {}", partition.getKey(), cursor);
        final ZKSubscription subscription = getZk().subscribeForOffsetChanges(
                partition.getKey(),
                () -> addTask(() -> offsetChanged(partition.getKey())));
        final PartitionData pd = new PartitionData(
                subscription,
                cursor,
                LoggerFactory.getLogger("subscription." + getSessionId() + "." + partition.getKey()),
                System.currentTimeMillis());

        offsets.put(partition.getKey(), pd);
    }

    private void reassignCommitted() {
        final List<EventTypePartition> keysToRelease = releasingPartitions.keySet().stream()
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

    void offsetChanged(final EventTypePartition key) {
        if (offsets.containsKey(key)) {
            final PartitionData data = offsets.get(key);
            data.getSubscription().refresh();

            final NakadiCursor cursor = createNakadiCursor(getZk().getOffset(key));

            final PartitionData.CommitResult commitResult = data.onCommitOffset(cursor);
            if (commitResult.seekOnKafka) {
                reconfigureKafkaConsumer(true);
            }

	    if (commitResult.committedCount > 0) {
                committedEvents += commitResult.committedCount;
                this.lastCommitMillis = System.currentTimeMillis();
                streamToOutput();
            }
            if (getParameters().isStreamLimitReached(committedEvents)) {
                final String debugMessage = "Stream limit in events reached: " + committedEvents;
                sendMetadata(debugMessage);
                shutdownGracefully(debugMessage);
            }
            if (releasingPartitions.containsKey(key) && data.isCommitted()) {
                reassignCommitted();
                logPartitionAssignment("New offset received for releasing partition " + key);
            }
        }
    }

    private void removeFromStreaming(final EventTypePartition key) {
        getLog().info("Removing partition {} from streaming", key);
        releasingPartitions.remove(key);
        final PartitionData data = offsets.remove(key);
        if (null != data) {
            try {
                if (data.getUnconfirmed() > 0) {
                    getLog().warn("Skipping commits: {}, commit={}, sent={}",
                            key, data.getCommitOffset(), data.getSentOffset());
                }
                data.getSubscription().cancel();
            } catch (final RuntimeException ex) {
                getLog().warn("Failed to cancel subscription, skipping exception", ex);
            }
        }
    }
}
