package org.zalando.nakadi.service.subscription.state;

import com.codahale.metrics.Meter;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zalando.nakadi.domain.ConsumedEvent;
import org.zalando.nakadi.domain.EventTypePartition;
import org.zalando.nakadi.domain.NakadiCursor;
import org.zalando.nakadi.domain.PartitionStatistics;
import org.zalando.nakadi.domain.Timeline;
import org.zalando.nakadi.exceptions.runtime.InternalNakadiException;
import org.zalando.nakadi.exceptions.runtime.InvalidCursorException;
import org.zalando.nakadi.exceptions.runtime.NakadiRuntimeException;
import org.zalando.nakadi.exceptions.runtime.ServiceTemporarilyUnavailableException;
import org.zalando.nakadi.metrics.MetricUtils;
import org.zalando.nakadi.repository.EventConsumer;
import org.zalando.nakadi.service.subscription.IdleStreamWatcher;
import org.zalando.nakadi.service.subscription.model.Partition;
import org.zalando.nakadi.service.subscription.zk.ZkSubscription;
import org.zalando.nakadi.service.subscription.zk.ZkSubscriptionClient;
import org.zalando.nakadi.view.SubscriptionCursor;
import org.zalando.nakadi.view.SubscriptionCursorWithoutToken;

import java.io.Closeable;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
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

import static java.util.stream.Collectors.groupingBy;


class StreamingState extends State {
    private static final Logger LOG = LoggerFactory.getLogger(StreamingState.class);

    private final Map<EventTypePartition, PartitionData> offsets = new HashMap<>();
    // Maps partition barrier when releasing must be completed or stream will be closed.
    // The reasons for that if there are two partitions (p0, p1) and p0 is reassigned, if p1 is working
    // correctly, and p0 is not receiving any updates - reassignment won't complete.
    private final Map<EventTypePartition, Long> releasingPartitions = new HashMap<>();
    private ZkSubscription<ZkSubscriptionClient.Topology> topologyChangeSubscription;
    private EventConsumer.ReassignableEventConsumer eventConsumer;
    private boolean pollPaused;
    private long committedEvents;
    private long sentEvents;
    private long batchesSent;
    private Meter bytesSentMeterPerSubscription;
    // Uncommitted offsets are calculated right on exiting from Streaming state.
    private Map<EventTypePartition, NakadiCursor> uncommittedOffsets;
    private Closeable cursorResetSubscription;
    private IdleStreamWatcher idleStreamWatcher;
    private boolean commitTimeoutReached = false;

    /**
     * Time that is used for commit timeout check. Commit timeout check is working only in case when there is something
     * uncommitted. Value is updated in 2 cases: <ul>
     * <li>something was committed</li>
     * <li>stream moves from state when everything was committed to state with uncommitted events</li>
     * </ul>
     */
    private long lastCommitMillis;

    private static final long AUTOCOMMIT_INTERVAL_SECONDS = 5;

    /**
     * 1. Collects names and prepares to send metrics for bytes streamed
     * <p>
     * 2. On entering, triggers re-balance for this session
     * <p>
     * 3. Refreshes topology and sends headers to the user
     * <p>
     * 4. Adds tasks for actions if change in topology, sessions list and cursor reset
     * <p>
     * 5. Schedules tasks for providing events batch to output based on params, and for checking commit timeouts
     */
    @Override
    public void onEnter() {
        final String kafkaFlushedBytesMetricName = MetricUtils.metricNameForHiLAStream(
                this.getContext().getParameters().getConsumingClient().getClientId(),
                this.getContext().getSubscription().getId()
        );
        bytesSentMeterPerSubscription = this.getContext().getMetricRegistry().meter(kafkaFlushedBytesMetricName);

        addTask(() -> getContext().subscribeToSessionListChangeAndRebalance());

        idleStreamWatcher = new IdleStreamWatcher(getParameters().commitTimeoutMillis * 2);
        this.eventConsumer = getContext().getTimelineService().createEventConsumer(null);

        recreateTopologySubscription();
        addTask(this::recheckTopology);
        addTask(this::initializeStream);
        addTask(this::pollDataFromKafka);
        scheduleTask(this::checkBatchTimeouts, getParameters().batchTimeoutMillis, TimeUnit.MILLISECONDS);
        scheduleTask(this::autocommitPeriodically, AUTOCOMMIT_INTERVAL_SECONDS, TimeUnit.SECONDS);

        scheduleTask(() -> {
                    streamToOutput(true);
                    final String debugMessage = "Stream timeout reached";
                    sendMetadata(debugMessage);
                    shutdownGracefully(debugMessage);
                }, getParameters().streamTimeoutMillis,
                TimeUnit.MILLISECONDS);

        this.lastCommitMillis = System.currentTimeMillis();
        scheduleTask(this::checkCommitTimeout, getParameters().commitTimeoutMillis, TimeUnit.MILLISECONDS);

        cursorResetSubscription = getZk().subscribeForStreamClose(
                () -> addTask(this::resetSubscriptionCursorsCallback));
    }

    private void autocommitPeriodically() {
        getAutocommit().autocommit();
        scheduleTask(this::autocommitPeriodically, AUTOCOMMIT_INTERVAL_SECONDS, TimeUnit.SECONDS);
    }

    private void recreateTopologySubscription() {
        if (null != topologyChangeSubscription) {
            topologyChangeSubscription.close();
        }
        topologyChangeSubscription = getZk().subscribeForTopologyChanges(() -> addTask(this::reactOnTopologyChange));
        reactOnTopologyChange();
    }

    private void initializeStream() {
        try {
            getOut().onInitialized(getSessionId());
        } catch (final IOException e) {
            LOG.error("Failed to notify of initialization. Switch to cleanup directly", e);
            switchState(new CleanupState(e));
        }
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
                this.commitTimeoutReached = true;
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
        logStreamCloseReason("Shutting down gracefully: " + reason);
        LOG.info("Shutting down gracefully. Reason: {}", reason);
        switchState(new ClosingState(this::getUncommittedOffsets, this::getLastCommitMillis));
    }

    @VisibleForTesting
    void pollDataFromKafka() {
        if (eventConsumer == null) {
            throw new IllegalStateException("kafkaConsumer should not be null when calling pollDataFromKafka method");
        }

        if (getContext().isSubscriptionConsumptionBlocked()) {
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
        final PartitionData pd = offsets.get(event.getPosition().getEventTypePartition());
        if (null != pd) {
            if (getContext().isConsumptionBlocked(event)) {
                getContext().getAutocommitSupport().addSkippedEvent(event.getPosition());
            } else {
                pd.addEvent(event);
            }
        }
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
            LOG.debug("Probably acting too slow, stream timeouts are constantly rescheduled");
            addTask(this::checkBatchTimeouts);
        }
    }

    private void streamToOutput() {
        streamToOutput(false);
    }

    private void streamToOutput(final boolean streamTimeoutReached) {
        final long currentTimeMillis = System.currentTimeMillis();
        int messagesAllowedToSend = (int) getMessagesAllowedToSend();
        final boolean wasCommitted = isEverythingCommitted();
        boolean sentSomething = false;

        for (final Map.Entry<EventTypePartition, PartitionData> e : offsets.entrySet()) {
            List<ConsumedEvent> toSend;
            while (null != (toSend = e.getValue().takeEventsToStream(
                    currentTimeMillis,
                    Math.min(getParameters().batchLimitEvents, messagesAllowedToSend),
                    getParameters().batchTimeoutMillis,
                    streamTimeoutReached))) {
                sentSomething |= !toSend.isEmpty();
                flushData(e.getKey(), toSend, batchesSent == 0 ? Optional.of("Stream started") : Optional.empty());
                if (toSend.isEmpty()) {
                    break;
                }
                messagesAllowedToSend -= toSend.size();
            }
        }

        long memoryConsumed = offsets.values().stream().mapToLong(PartitionData::getBytesInMemory).sum();
        while (memoryConsumed > getContext().getStreamMemoryLimitBytes() && messagesAllowedToSend > 0) {
            // Select heaviest guy (and on previous step we figured out that we can not send anymore full batches,
            // therefore we can take all the events from one partition.
            final Map.Entry<EventTypePartition, PartitionData> heaviestPartition = offsets.entrySet().stream().max(
                    Comparator.comparing(e -> e.getValue().getBytesInMemory())
            ).get(); // There is always at least 1 item in list

            long deltaSize = heaviestPartition.getValue().getBytesInMemory();
            final List<ConsumedEvent> events = heaviestPartition.getValue().extractMaxEvents(currentTimeMillis,
                    messagesAllowedToSend);
            deltaSize -= heaviestPartition.getValue().getBytesInMemory();

            sentSomething = true;
            flushData(
                    heaviestPartition.getKey(),
                    events,
                    Optional.of("Stream parameters are causing overflow"));
            messagesAllowedToSend -= events.size();
            LOG.info("Memory limit reached: {} bytes. Dumped events from {}. Freed: {} bytes, {} messages",
                    memoryConsumed, heaviestPartition.getKey(), deltaSize, events.size());
            memoryConsumed -= deltaSize;
        }

        getContext().getKpiCollector().checkAndSendKpi();

        if (wasCommitted && sentSomething) {
            this.lastCommitMillis = System.currentTimeMillis();
        }
        pollPaused = messagesAllowedToSend <= 0;
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

            final long batchSizeBytes = getContext().getWriter().writeSubscriptionBatch(
                    getOut().getOutputStream(),
                    cursor,
                    data,
                    metadata);

            bytesSentMeterPerSubscription.mark(batchSizeBytes);

            getContext().getKpiCollector().recordBatchSent(pk.getEventType(), batchSizeBytes, data.size());

            batchesSent++;
            sentEvents += data.size();
        } catch (final IOException e) {
            shutdownGracefully("Failed to write data to output: " + e);
        }
    }

    public void logExtendedCommitInformation() {
        // We need to log situation when commit timeout was reached, and check that current committed offset is the
        // same as it is in zk.
        if (!commitTimeoutReached) {
            return;
        }
        final List<EventTypePartition> toCheck = offsets.entrySet().stream()
                .filter(e -> !e.getValue().isCommitted())
                .map(Map.Entry::getKey)
                .collect(Collectors.toList());
        try {
            final Map<EventTypePartition, SubscriptionCursorWithoutToken> realCommitted = getZk().getOffsets(toCheck);
            final List<EventTypePartition> bustedPartitions = offsets.entrySet().stream()
                    .filter(v -> realCommitted.containsKey(v.getKey()))
                    .filter(v -> {
                        final SubscriptionCursorWithoutToken remembered =
                                getContext().getCursorConverter().convertToNoToken(v.getValue().getCommitOffset());
                        final SubscriptionCursorWithoutToken real = realCommitted.get(v.getKey());
                        return real.getOffset().compareTo(remembered.getOffset()) > 0;
                    })
                    .map(Map.Entry::getKey)
                    .collect(Collectors.toList());
            if (!bustedPartitions.isEmpty()) {
                final String bustedData = bustedPartitions.stream().map(etp -> {
                    final PartitionData pd = offsets.get(etp);
                    return "(ETP: " + etp +
                            ", StreamCommitted: " + pd.getCommitOffset() +
                            ", StreamSent: " + pd.getSentOffset() +
                            ", ZkCommitted: " + realCommitted.get(etp) + ")";
                }).collect(Collectors.joining(", "));
                LOG.warn("Stale offsets during streaming commit timeout: {}", bustedData);
            }
        } catch (NakadiRuntimeException ex) {
            LOG.warn("Failed to get nakadi cursors for logging purposes.");
        }
    }

    @Override
    public void onExit() {
        uncommittedOffsets = offsets.entrySet().stream()
                .filter(e -> !e.getValue().isCommitted())
                .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().getSentOffset()));

        getContext().getKpiCollector().sendKpi();
        logExtendedCommitInformation();
        if (null != topologyChangeSubscription) {
            try {
                topologyChangeSubscription.close();
            } catch (final RuntimeException ex) {
                LOG.warn("Failed to cancel topology subscription", ex);
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
            try {
                cursorResetSubscription.close();
            } catch (IOException ignore) {
            }
            cursorResetSubscription = null;
        }
    }

    void reactOnTopologyChange() {
        if (null == topologyChangeSubscription) {
            return;
        }
        final ZkSubscriptionClient.Topology topology = topologyChangeSubscription.getData();
        final Partition[] assignedPartitions = Stream.of(topology.getPartitions())
                .filter(p -> getSessionId().equals(p.getSession()))
                .toArray(Partition[]::new);
        addTask(() -> refreshTopologyUnlocked(assignedPartitions));
        trackIdleness(topology);
    }

    void recheckTopology() {
        // Sometimes topology is not refreshed. One need to explicitly check that topology is still valid.
        final Partition[] partitions = Stream.of(getZk().getTopology().getPartitions())
                .filter(p -> getSessionId().equalsIgnoreCase(p.getSession()))
                .toArray(Partition[]::new);
        if (refreshTopologyUnlocked(partitions)) {
            LOG.info("Topology changed not by event, but by schedule. Recreating zk listener");
            recreateTopologySubscription();
        }
        // addTask() is used to check if state is not changed.
        scheduleTask(() -> addTask(this::recheckTopology), getParameters().commitTimeoutMillis, TimeUnit.MILLISECONDS);
    }

    boolean refreshTopologyUnlocked(final Partition[] assignedPartitions) {
        final Map<EventTypePartition, Partition> newAssigned = Stream.of(assignedPartitions)
                .filter(p -> p.getState() == Partition.State.ASSIGNED)
                .collect(Collectors.toMap(Partition::getKey, p -> p));

        final Map<EventTypePartition, Partition> newReassigning = Stream.of(assignedPartitions)
                .filter(p -> p.getState() == Partition.State.REASSIGNING)
                .collect(Collectors.toMap(Partition::getKey, p -> p));

        boolean modified = false;

        // 1. Select which partitions must be removed right now for some (strange) reasons
        // (no need to release topology).
        final List<EventTypePartition> forciblyRemoved = offsets.keySet().stream()
                .filter(e -> !newAssigned.containsKey(e))
                .filter(e -> !newReassigning.containsKey(e))
                .collect(Collectors.toList());
        if (!forciblyRemoved.isEmpty()) {
            modified = true;
            forciblyRemoved.forEach(this::removeFromStreaming);
        }
        // 2. Clear releasing partitions that was returned to this session.
        final List<EventTypePartition> removedFromReassign =
                releasingPartitions.keySet().stream()
                        .filter(p -> !newReassigning.containsKey(p))
                        .collect(Collectors.toList());
        if (!removedFromReassign.isEmpty()) {
            modified = true;
            removedFromReassign.forEach(releasingPartitions::remove);
        }

        // 3. Add releasing partitions information
        modified |= !newReassigning.keySet().stream()
                .filter(this::addPartitionToReassigned)
                .collect(Collectors.toList()).isEmpty();

        // 4. Select which partitions must be added and add it.
        final List<Partition> newAssignedPartitions = newAssigned.values()
                .stream()
                .filter(p -> !offsets.containsKey(p.getKey()))
                .collect(Collectors.toList());
        if (!newAssignedPartitions.isEmpty()) {
            modified = true;
            final Map<EventTypePartition, SubscriptionCursorWithoutToken> committed = getZk().getOffsets(
                    newAssignedPartitions.stream().map(Partition::getKey).collect(Collectors.toList()));
            newAssignedPartitions.forEach(p -> this.addToStreaming(p, committed));
        }
        // 5. Check if something can be released right now
        if (modified) {
            reassignCommitted();

            // 6. Reconfigure kafka consumer
            reconfigureKafkaConsumer(false);

            logPartitionAssignment("Topology refreshed");
        }
        return modified;
    }

    private void logPartitionAssignment(final String reason) {
        if (LOG.isInfoEnabled()) {
            LOG.info("{}. Streaming partitions: [{}]. Reassigning partitions: [{}]",
                    reason,
                    offsets.keySet().stream().filter(p -> !releasingPartitions.containsKey(p))
                            .map(EventTypePartition::toString).collect(Collectors.joining(",")),
                    releasingPartitions.keySet().stream().map(EventTypePartition::toString)
                            .collect(Collectors.joining(", ")));
        }
    }

    private boolean addPartitionToReassigned(final EventTypePartition partitionKey) {
        if (!releasingPartitions.containsKey(partitionKey)) {
            final long currentTime = System.currentTimeMillis();
            final long barrier = currentTime + getParameters().commitTimeoutMillis;
            releasingPartitions.put(partitionKey, barrier);
            scheduleTask(() -> barrierOnRebalanceReached(partitionKey), getParameters().commitTimeoutMillis,
                    TimeUnit.MILLISECONDS);
            return true;
        }
        return false;
    }

    private void barrierOnRebalanceReached(final EventTypePartition pk) {
        if (!releasingPartitions.containsKey(pk)) {
            return;
        }
        LOG.info("Checking barrier to transfer partition {}", pk);
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
            } catch (final InvalidCursorException ex) {
                throw new NakadiRuntimeException(ex);
            }
        }
        final Set<EventTypePartition> currentAssignment = eventConsumer.getAssignment();

        LOG.info("Changing kafka assignment from {} to {}",
                Arrays.deepToString(currentAssignment.toArray()),
                Arrays.deepToString(newAssignment.toArray()));

        if (!currentAssignment.equals(newAssignment)) {
            try {
                final Map<EventTypePartition, NakadiCursor> beforeFirst = getBeforeFirstCursors(newAssignment);
                final List<NakadiCursor> cursors = newAssignment.stream()
                        .map(pk -> {
                            final NakadiCursor beforeFirstAvailable = beforeFirst.get(pk);

                            // Checks that current cursor is still available in storage. Otherwise reset to oldest
                            // available offset for the partition
                            offsets.get(pk).ensureDataAvailable(beforeFirstAvailable);
                            return offsets.get(pk).getSentOffset();
                        })
                        .collect(Collectors.toList());
                eventConsumer.reassign(cursors);
            } catch (InvalidCursorException ex) {
                throw new NakadiRuntimeException(ex);
            }
        }
    }

    private Map<EventTypePartition, NakadiCursor> getBeforeFirstCursors(final Set<EventTypePartition> newAssignment) {
        return newAssignment.stream()
                .map(EventTypePartition::getEventType)
                .map(et -> {
                    try {
                        // get oldest active timeline
                        return getContext().getTimelineService().getActiveTimelinesOrdered(et).get(0);
                    } catch (final InternalNakadiException e) {
                        throw new NakadiRuntimeException(e);
                    }
                })
                .collect(groupingBy(Timeline::getStorage)) // for performance reasons. See ARUHA-1387
                .values()
                .stream()
                .flatMap(timelines -> {
                    try {
                        return getContext().getTimelineService().getTopicRepository(timelines.get(0))
                                .loadTopicStatistics(timelines).stream();
                    } catch (final ServiceTemporarilyUnavailableException e) {
                        throw new NakadiRuntimeException(e);
                    }
                })
                .map(PartitionStatistics::getBeforeFirst)
                .collect(Collectors.toMap(NakadiCursor::getEventTypePartition, cursor -> cursor));
    }

    private NakadiCursor createNakadiCursor(final SubscriptionCursorWithoutToken cursor) {
        try {
            return getContext().getCursorConverter().convert(cursor);
        } catch (final Exception ex) {
            throw new NakadiRuntimeException(ex);
        }
    }

    private void addToStreaming(final Partition partition,
                                final Map<EventTypePartition, SubscriptionCursorWithoutToken> cursorMap) {
        final NakadiCursor cursor = createNakadiCursor(cursorMap.get(partition.getKey()));
        LOG.info("Adding to streaming {} with start position {}", partition.getKey(), cursor);
        final ZkSubscription<SubscriptionCursorWithoutToken> subscription = getZk().subscribeForOffsetChanges(
                partition.getKey(),
                () -> addTask(() -> offsetChanged(partition.getKey())));
        final PartitionData pd = new PartitionData(
                getComparator(),
                subscription,
                cursor,
                System.currentTimeMillis(),
                this.getContext().getParameters().batchTimespan,
                getContext().getCursorOperationsService()
        );

        offsets.put(partition.getKey(), pd);
        getAutocommit().addPartition(cursor);
    }

    private void reassignCommitted() {
        final List<EventTypePartition> keysToRelease = releasingPartitions.keySet().stream()
                .filter(pk -> !offsets.containsKey(pk) || offsets.get(pk).isCommitted())
                .collect(Collectors.toList());
        if (!keysToRelease.isEmpty()) {
            try {
                keysToRelease.forEach(this::removeFromStreaming);
            } finally {
                getZk().transfer(getSessionId(), keysToRelease);
            }
        }
    }

    void offsetChanged(final EventTypePartition key) {
        if (offsets.containsKey(key)) {
            final PartitionData data = offsets.get(key);

            final NakadiCursor cursor = createNakadiCursor(data.getSubscription().getData());

            final PartitionData.CommitResult commitResult = data.onCommitOffset(cursor);
            getAutocommit().onCommit(cursor);

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
        LOG.info("Removing partition {} from streaming", key);
        releasingPartitions.remove(key);
        final PartitionData data = offsets.remove(key);
        getAutocommit().removePartition(key);
        if (null != data) {
            try {
                if (data.getUnconfirmed() > 0) {
                    LOG.info("Skipping commits: {}, commit={}, sent={}",
                            key, data.getCommitOffset(), data.getSentOffset());
                }
                data.getSubscription().close();
            } catch (final RuntimeException ex) {
                LOG.warn("Failed to cancel subscription, skipping exception", ex);
            }
        }
    }

    /**
     * If stream doesn't have any partitions - start timer that will close this session
     * in commitTimeout*2 if it doesn't get any partitions during that time
     *
     * @param topology the new topology
     */
    private void trackIdleness(final ZkSubscriptionClient.Topology topology) {
        final boolean hasAnyAssignment = Stream.of(topology.getPartitions())
                .anyMatch(p -> getSessionId().equals(p.getSession()) || getSessionId().equals(p.getNextSession()));
        if (hasAnyAssignment) {
            idleStreamWatcher.idleEnd();
        } else {
            final boolean justSwitchedToIdle = idleStreamWatcher.idleStart();
            if (justSwitchedToIdle) {
                scheduleTask(() -> {
                    if (idleStreamWatcher.isIdleForToolLong()) {
                        shutdownGracefully("There are no available partitions to read");
                    }
                }, idleStreamWatcher.getIdleCloseTimeout(), TimeUnit.MILLISECONDS);
            }
        }
    }

}
