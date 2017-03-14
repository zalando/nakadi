package org.zalando.nakadi.service.subscription.state;

import com.codahale.metrics.Meter;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.base.Preconditions;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
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
import org.zalando.nakadi.domain.NakadiCursor;
import org.zalando.nakadi.domain.PartitionStatistics;
import org.zalando.nakadi.domain.Timeline;
import org.zalando.nakadi.domain.TopicPartition;
import org.zalando.nakadi.exceptions.InternalNakadiException;
import org.zalando.nakadi.exceptions.InvalidCursorException;
import org.zalando.nakadi.exceptions.NakadiException;
import org.zalando.nakadi.exceptions.NakadiRuntimeException;
import org.zalando.nakadi.exceptions.NoSuchEventTypeException;
import org.zalando.nakadi.exceptions.ServiceUnavailableException;
import org.zalando.nakadi.metrics.MetricUtils;
import org.zalando.nakadi.repository.EventConsumer;
import org.zalando.nakadi.service.EventStream;
import org.zalando.nakadi.service.subscription.model.Partition;
import org.zalando.nakadi.service.subscription.zk.ZKSubscription;
import org.zalando.nakadi.view.SubscriptionCursor;


class StreamingState extends State {
    private final Map<TopicPartition, PartitionData> offsets = new HashMap<>();
    // Maps partition barrier when releasing must be completed or stream will be closed.
    // The reasons for that if there are two partitions (p0, p1) and p0 is reassigned, if p1 is working
    // correctly, and p0 is not receiving any updates - reassignment won't complete.
    private final Map<TopicPartition, Long> releasingPartitions = new HashMap<>();
    private ZKSubscription topologyChangeSubscription;
    private EventConsumer.ReassignableEventConsumer eventConsumer;
    private boolean pollPaused;
    private long lastCommitMillis;
    private long committedEvents;
    private long sentEvents;
    private long batchesSent;
    private Meter bytesSentMeter;
    // Uncommitted offsets are calculated right on exiting from Streaming state.
    private Map<TopicPartition, NakadiCursor> uncommittedOffsets;

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
    }

    private void checkCommitTimeout() {
        final long currentMillis = System.currentTimeMillis();
        final boolean hasUncommitted = offsets.values().stream().anyMatch(d -> !d.isCommitted());
        if (hasUncommitted) {
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

    private void sendMetadata(final String metadata) {
        offsets.entrySet().stream().findFirst()
                .ifPresent(pk -> flushData(pk.getKey(), Collections.emptyList(), Optional.of(metadata)));
    }

    private long getLastCommitMillis() {
        return lastCommitMillis;
    }

    private Map<TopicPartition, NakadiCursor> getUncommittedOffsets() {
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
        Optional.ofNullable(offsets.get(event.getPosition().getTopicPartition())).ifPresent(pd -> pd.addEvent(event));
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
        int freeSlots = (int) getMessagesAllowedToSend();
        for (final Map.Entry<TopicPartition, PartitionData> e : offsets.entrySet()) {
            List<ConsumedEvent> toSend;
            while (null != (toSend = e.getValue().takeEventsToStream(
                    currentTimeMillis,
                    Math.min(getParameters().batchLimitEvents, freeSlots),
                    getParameters().batchTimeoutMillis))) {
                flushData(e.getKey(), toSend, batchesSent == 0 ? Optional.of("Stream started") : Optional.empty());
                this.sentEvents += toSend.size();
                if (toSend.isEmpty()) {
                    break;
                }
                freeSlots -= toSend.size();
            }
        }
        pollPaused = getMessagesAllowedToSend() <= 0;
        if (!offsets.isEmpty() &&
                getParameters().isKeepAliveLimitReached(offsets.values().stream()
                        .mapToInt(PartitionData::getKeepAliveInARow))) {
            shutdownGracefully("All partitions reached keepAlive limit");
        }
    }

    private void flushData(final TopicPartition pk, final List<ConsumedEvent> data,
                           final Optional<String> metadata) {
        try {
            final NakadiCursor sentOffset = offsets.get(pk).getSentOffset();
            final String batch = serializeBatch(sentOffset, data, metadata);

            final byte[] batchBytes = batch.getBytes(EventStream.UTF8);
            getOut().streamData(batchBytes);
            bytesSentMeter.mark(batchBytes.length);
            batchesSent++;
        } catch (final IOException e) {
            getLog().error("Failed to write data to output.", e);
            shutdownGracefully("Failed to write data to output");
        }
    }

    private String serializeBatch(final NakadiCursor lastSentCursor, final List<ConsumedEvent> events,
                                  final Optional<String> metadata)
            throws JsonProcessingException {

        final SubscriptionCursor cursor = getContext().getCursorConverter().convert(
                lastSentCursor,
                getContext().getCursorTokenService().generateToken());
        final String cursorSerialized = getContext().getObjectMapper().writeValueAsString(cursor);

        final StringBuilder builder = new StringBuilder()
                .append("{\"cursor\":")
                .append(cursorSerialized);
        if (!events.isEmpty()) {
            builder.append(",\"events\":[");
            events.forEach(event -> builder.append(event.getEvent()).append(","));
            builder.deleteCharAt(builder.length() - 1).append("]");
        }
        metadata.ifPresent(s -> builder.append(",\"info\":{\"debug\":\"").append(s).append("\"}"));

        builder.append("}").append(EventStream.BATCH_SEPARATOR);
        return builder.toString();
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
            } catch (IOException e) {
                throw new NakadiRuntimeException(e);
            } finally {
                eventConsumer = null;
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
        final Map<TopicPartition, Partition> newAssigned = Stream.of(assignedPartitions)
                .filter(p -> p.getState() == Partition.State.ASSIGNED)
                .collect(Collectors.toMap(Partition::getKey, p -> p));

        final Map<TopicPartition, Partition> newReassigning = Stream.of(assignedPartitions)
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
                            .map(TopicPartition::toString).collect(Collectors.joining(",")),
                    releasingPartitions.keySet().stream().map(TopicPartition::toString)
                            .collect(Collectors.joining(", ")));
        }
    }

    private void addPartitionToReassigned(final TopicPartition partitionKey) {
        final long currentTime = System.currentTimeMillis();
        final long barrier = currentTime + getParameters().commitTimeoutMillis;
        releasingPartitions.put(partitionKey, barrier);
        scheduleTask(() -> barrierOnRebalanceReached(partitionKey), getParameters().commitTimeoutMillis,
                TimeUnit.MILLISECONDS);
    }

    private void barrierOnRebalanceReached(final TopicPartition pk) {
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
        final Set<TopicPartition> newAssignment = offsets.keySet().stream()
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
        final Set<TopicPartition> currentKafkaAssignment = eventConsumer.getAssignment().stream()
                .map(tp -> new TopicPartition(tp.getTopic(), String.valueOf(tp.getPartition())))
                .collect(Collectors.toSet());

        getLog().info("Changing kafka assignment from {} to {}",
                Arrays.deepToString(currentKafkaAssignment.toArray()),
                Arrays.deepToString(newAssignment.toArray()));

        if (!currentKafkaAssignment.equals(newAssignment)) {
            try {
                final Map<TopicPartition, Timeline> temporaryMapping = loadPartitionsMapping(newAssignment);
                final List<NakadiCursor> cursors = new ArrayList<>();
                for (final TopicPartition pk : newAssignment) {
                    // Next 2 lines checks that current cursor is still available in storage
                    final NakadiCursor beforeFirstAvailable = getBeforeFirstCursor(temporaryMapping, pk);
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

    private NakadiCursor getBeforeFirstCursor(final Map<TopicPartition, Timeline> temporaryMapping,
                                              final TopicPartition pk)
            throws InternalNakadiException, NoSuchEventTypeException, ServiceUnavailableException {
        final Timeline timeline = temporaryMapping.get(pk);
        final Timeline firstTimelineForET = getContext().getTimelineService()
                .getActiveTimelinesOrdered(timeline.getEventType()).get(0);

        final Optional<PartitionStatistics> stats = getContext().getTimelineService()
                .getTopicRepository(firstTimelineForET)
                .loadPartitionStatistics(firstTimelineForET, pk.getPartition());

        return stats.get().getBeforeFirst();
    }

    private Map<TopicPartition, Timeline> loadPartitionsMapping(
            final Collection<TopicPartition> partitions) throws InternalNakadiException, NoSuchEventTypeException {
        final Map<TopicPartition, Timeline> result = new HashMap<>();
        for (final String et : getContext().getSubscription().getEventTypes()) {
            final List<Timeline> timelines = getContext().getTimelineService().getActiveTimelinesOrdered(et);
            for (final Timeline timeline : timelines) {
                final List<TopicPartition> matchingPartitions = partitions.stream()
                        .filter(p -> p.getTopic().equals(timeline.getTopic()))
                        .collect(Collectors.toList());

                for (final TopicPartition pk : matchingPartitions) {
                    result.put(pk, timeline);
                }
            }
        }
        return result;
    }

    private NakadiCursor createNakadiCursor(final TopicPartition key, final String offset) {
        final Timeline timeline;
        try {
            timeline = loadPartitionsMapping(Collections.singletonList(key)).get(key);
        } catch (InternalNakadiException | NoSuchEventTypeException e) {
            throw new NakadiRuntimeException(e);
        }
        return new NakadiCursor(timeline, key.getPartition(), offset);
    }

    private void addToStreaming(final Partition partition) {
        final NakadiCursor cursor = createNakadiCursor(partition.getKey(), getZk().getOffset(partition.getKey()));
        getLog().info("Adding to streaming {} with start position {}", partition.getKey(), cursor);
        final ZKSubscription subscription = getZk().subscribeForOffsetChanges(
                partition.getKey(),
                () -> addTask(() -> offsetChanged(partition.getKey())));
        final PartitionData pd = new PartitionData(
                subscription,
                cursor,
                LoggerFactory.getLogger("subscription." + getSessionId() + "." + partition.getKey()));

        offsets.put(partition.getKey(), pd);
    }

    private void reassignCommitted() {
        final List<TopicPartition> keysToRelease = releasingPartitions.keySet().stream()
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

    void offsetChanged(final TopicPartition key) {
        if (offsets.containsKey(key)) {
            final PartitionData data = offsets.get(key);
            data.getSubscription().refresh();

            final NakadiCursor cursor = createNakadiCursor(key, getZk().getOffset(key));

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

    private void removeFromStreaming(final TopicPartition key) {
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
