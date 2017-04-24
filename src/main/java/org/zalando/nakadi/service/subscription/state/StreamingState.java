package org.zalando.nakadi.service.subscription.state;

import com.codahale.metrics.Meter;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import java.nio.charset.StandardCharsets;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.LoggerFactory;
import org.zalando.nakadi.metrics.MetricUtils;
import org.zalando.nakadi.service.EventStream;
import org.zalando.nakadi.service.subscription.SubscriptionOutput;
import org.zalando.nakadi.service.subscription.model.Partition;
import org.zalando.nakadi.service.subscription.zk.ZKSubscription;
import org.zalando.nakadi.util.FeatureToggleService;
import org.zalando.nakadi.view.SubscriptionCursor;
import org.zalando.nakadi.domain.Timeline;

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
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;


class StreamingState extends State {

    private static final byte[] B_CURSOR_START = "{\"cursor\":".getBytes(StandardCharsets.UTF_8);
    private static final byte[] B_EVENTS_START = ",\"events\":[".getBytes(StandardCharsets.UTF_8);
    private static final byte[] B_COMMA = ",".getBytes(StandardCharsets.UTF_8);
    private static final byte[] B_CLOSE_BRACKET = "]".getBytes(StandardCharsets.UTF_8);
    private static final byte[] B_METADATA_INFO_DEBUG = ",\"info\":{\"debug\":\"".getBytes(StandardCharsets.UTF_8);
    private static final byte[] B_QUOTE_CLOSE_BRACE = "\"}".getBytes(StandardCharsets.UTF_8);
    private static final byte[] B_CLOSE_BRACE = "}".getBytes(StandardCharsets.UTF_8);

    private final Map<Partition.PartitionKey, PartitionData> offsets = new HashMap<>();
    // Maps partition barrier when releasing must be completed or stream will be closed.
    // The reasons for that if there are two partitions (p0, p1) and p0 is reassigned, if p1 is working
    // correctly, and p0 is not receiving any updates - reassignment won't complete.
    private final Map<Partition.PartitionKey, Long> releasingPartitions = new HashMap<>();
    private ZKSubscription topologyChangeSubscription;
    private Consumer<String, String> kafkaConsumer;
    private boolean pollPaused;
    private long lastCommitMillis;
    private long committedEvents;
    private long sentEvents;
    private long batchesSent;
    private Meter bytesSentMeter;
    // Uncommitted offsets are calculated right on exiting from Streaming state.
    private Map<Partition.PartitionKey, Long> uncommittedOffsets;
    private ZKSubscription cursorResetSubscription;

    @Override
    public void onEnter() {
        final String kafkaFlushedBytesMetricName = MetricUtils.metricNameForHiLAStream(
                this.getContext().getParameters().getConsumingAppId(),
                this.getContext().getSubscriptionId()
        );
        bytesSentMeter = this.getContext().getMetricRegistry().meter(kafkaFlushedBytesMetricName);

        this.kafkaConsumer = getKafka().createKafkaConsumer();

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
                .ifPresent(pk -> flushData(pk.getKey(), new TreeMap<>(), Optional.of(metadata)));
    }

    private long getLastCommitMillis() {
        return lastCommitMillis;
    }

    private Map<Partition.PartitionKey, Long> getUncommittedOffsets() {
        Preconditions.checkNotNull(uncommittedOffsets, "uncommittedOffsets should not be null on time of call");
        return uncommittedOffsets;
    }

    private void shutdownGracefully(final String reason) {
        getLog().info("Shutting down gracefully. Reason: {}", reason);
        switchState(new ClosingState(this::getUncommittedOffsets, this::getLastCommitMillis));
    }

    private void pollDataFromKafka() {
        if (kafkaConsumer == null) {
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

        if (kafkaConsumer.assignment().isEmpty() || pollPaused) {
            // Small optimization not to waste CPU while not yet assigned to any partitions
            scheduleTask(this::pollDataFromKafka, getKafkaPollTimeout(), TimeUnit.MILLISECONDS);
            return;
        }
        final ConsumerRecords<String, String> records = kafkaConsumer.poll(getKafkaPollTimeout());
        if (!records.isEmpty()) {
            for (final TopicPartition tp : records.partitions()) {
                final Partition.PartitionKey pk = new Partition.PartitionKey(tp.topic(),
                        String.valueOf(tp.partition()));
                Optional.ofNullable(offsets.get(pk))
                        .ifPresent(pd -> records.records(tp)
                                .forEach(record -> pd.addEventFromKafka(record.offset(), record.value())));
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
        SortedMap<Long, String> toSend;
        for (final Map.Entry<Partition.PartitionKey, PartitionData> e : offsets.entrySet()) {
            while (null != (toSend = e.getValue().takeEventsToStream(
                    currentTimeMillis,
                    Math.min(getParameters().batchLimitEvents, messagesAllowedToSend),
                    getParameters().batchTimeoutMillis))) {
                flushData(e.getKey(), toSend, batchesSent == 0 ? Optional.of("Stream started") : Optional.empty());
                this.sentEvents += toSend.size();
                if (toSend.isEmpty()) {
                    break;
                }
                messagesAllowedToSend -= toSend.size();
            }
        }
        pollPaused = getMessagesAllowedToSend() <= 0;
        if (!offsets.isEmpty() &&
                getParameters().isKeepAliveLimitReached(offsets.values().stream()
                        .mapToInt(PartitionData::getKeepAliveInARow))) {
            shutdownGracefully("All partitions reached keepAlive limit");
        }
    }

    private void flushData(final Partition.PartitionKey pk, final SortedMap<Long, String> data,
                           final Optional<String> metadata) {
        final FeatureToggleService featureToggleService = getContext().getFeatureToggleService();
        if(featureToggleService.isFeatureEnabled(
            FeatureToggleService.Feature.SEND_SUBSCRIPTION_BATCH_VIA_OUTPUT_STREAM)) {
            try {

                final ObjectMapper mapper = getContext().getObjectMapper();
                final String token = getContext().getCursorTokenService().generateToken();
                final Timeline timeline = getContext().getTimelinesForTopics().get(pk.getTopic());
                final SubscriptionCursor cursor = getContext()
                    .getCursorConverter()
                    .convert(
                        pk.createKafkaCursor(offsets.get(pk).getSentOffset())
                            .toNakadiCursor(timeline), token
                    );

                writeStreamBatch(data, metadata, cursor, getOut(), mapper, bytesSentMeter);
            } catch (final IOException e) {
                getLog().error("Failed to write data to output.", e);
                shutdownGracefully("Failed to write data to output");
            }
        } else {
            try {
                final long numberOffset = offsets.get(pk).getSentOffset();
                final String batch = serializeBatch(pk, numberOffset, new ArrayList<>(data.values()), metadata);
                final byte[] batchBytes = batch.getBytes(EventStream.UTF8);
                getOut().streamData(batchBytes);
                bytesSentMeter.mark(batchBytes.length);
                batchesSent++;
            } catch (final IOException e) {
                getLog().error("Failed to write data to output.", e);
                shutdownGracefully("Failed to write data to output");
            }
        }
    }

    private String serializeBatch(final Partition.PartitionKey partitionKey, final long offset,
                                  final List<String> events, final Optional<String> metadata)
            throws JsonProcessingException {

        final Timeline timeline = getContext().getTimelinesForTopics().get(partitionKey.getTopic());
        final String token = getContext().getCursorTokenService().generateToken();
        final SubscriptionCursor cursor = getContext().getCursorConverter().convert(
                partitionKey.createKafkaCursor(offset).toNakadiCursor(timeline),
                token);
        final String cursorSerialized = getContext().getObjectMapper().writeValueAsString(cursor);

        final StringBuilder builder = new StringBuilder()
                .append("{\"cursor\":")
                .append(cursorSerialized);
        if (!events.isEmpty()) {
            builder.append(",\"events\":[");
            events.forEach(event -> builder.append(event).append(","));
            builder.deleteCharAt(builder.length() - 1).append("]");
        }
        metadata.ifPresent(s -> builder.append(",\"info\":{\"debug\":\"").append(s).append("\"}"));

        builder.append("}").append(EventStream.BATCH_SEPARATOR);
        return builder.toString();
    }

    @VisibleForTesting
    void writeStreamBatch(final SortedMap<Long, String> events,
        final Optional<String> infoLog,
        final SubscriptionCursor cursor,
        final SubscriptionOutput subscriptionOutput,
        final ObjectMapper mapper,
        final Meter bytesSentMeter
    )
            throws IOException {
        int byteCount = 0;

        final List<String> values = new ArrayList<>(events.values());

        subscriptionOutput.streamData(B_CURSOR_START);
        byteCount += B_CURSOR_START.length;

        // todo: sending direct to SubscriptionOutput's outputstream might be better here
        final byte[] cursorBytes = mapper.writeValueAsBytes(cursor);
        subscriptionOutput.streamData(cursorBytes);
        byteCount += cursorBytes.length;

        if (!values.isEmpty()) {
            subscriptionOutput.streamData(B_EVENTS_START);
            byteCount += B_EVENTS_START.length;
            for (int i = 0; i < values.size(); i++) {
                final byte[] event = values.get(i).getBytes(StandardCharsets.UTF_8);
                subscriptionOutput.streamData(event);
                byteCount += event.length;
                if(i < (values.size() - 1)) {
                    subscriptionOutput.streamData(B_COMMA);
                    byteCount += 1;
                }
                else {
                    subscriptionOutput.streamData(B_CLOSE_BRACKET);
                    byteCount += 1;
                }
            }
        }

        if(infoLog.isPresent()) {
            subscriptionOutput.streamData(B_METADATA_INFO_DEBUG);
            byteCount += B_METADATA_INFO_DEBUG.length;
            final byte[] metadataBytes = infoLog.get().getBytes(StandardCharsets.UTF_8);
            subscriptionOutput.streamData(metadataBytes);
            byteCount += metadataBytes.length;
            subscriptionOutput.streamData(B_QUOTE_CLOSE_BRACE);
            byteCount += B_QUOTE_CLOSE_BRACE.length;
        }

        subscriptionOutput.streamData(B_CLOSE_BRACE);
        byteCount += B_CLOSE_BRACE.length;
        final byte[] batchSeparator = EventStream.BATCH_SEPARATOR.getBytes(StandardCharsets.UTF_8);
        subscriptionOutput.streamData(batchSeparator);
        byteCount += batchSeparator.length;
        bytesSentMeter.mark(byteCount);
        batchesSent++;
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
        if (null != kafkaConsumer) {
            try {
                kafkaConsumer.close();
            } finally {
                kafkaConsumer = null;
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
        final Map<Partition.PartitionKey, Partition> newAssigned = Stream.of(assignedPartitions)
                .filter(p -> p.getState() == Partition.State.ASSIGNED)
                .collect(Collectors.toMap(Partition::getKey, p -> p));

        final Map<Partition.PartitionKey, Partition> newReassigning = Stream.of(assignedPartitions)
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
                            .map(Partition.PartitionKey::toString).collect(Collectors.joining(",")),
                    releasingPartitions.keySet().stream().map(Partition.PartitionKey::toString)
                            .collect(Collectors.joining(", ")));
        }
    }

    private void addPartitionToReassigned(final Partition.PartitionKey partitionKey) {
        final long currentTime = System.currentTimeMillis();
        final long barrier = currentTime + getParameters().commitTimeoutMillis;
        releasingPartitions.put(partitionKey, barrier);
        scheduleTask(() -> barrierOnRebalanceReached(partitionKey), getParameters().commitTimeoutMillis,
                TimeUnit.MILLISECONDS);
    }

    private void barrierOnRebalanceReached(final Partition.PartitionKey pk) {
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
        if (kafkaConsumer == null) {
            throw new IllegalStateException(
                    "kafkaConsumer should not be null when calling reconfigureKafkaConsumer method");
        }
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
                            k -> new TopicPartition(k.getTopic(), Integer.valueOf(k.getPartition()))));
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
                        getZk().subscribeForOffsetChanges(partition.getKey(), () -> addTask(()
                                -> offsetChanged(partition.getKey()))),
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

    private void removeFromStreaming(final Partition.PartitionKey key) {
        getLog().info("Removing partition {} from streaming", key);
        releasingPartitions.remove(key);
        final PartitionData data = offsets.remove(key);
        if (null != data) {
            try {
                if (data.getUnconfirmed() > 0) {
                    getLog().warn("Skipping commits: {}, commit={}, sent={}", key, data.getSentOffset()
                            - data.getUnconfirmed(), data.getSentOffset());
                }
                data.getSubscription().cancel();
            } catch (final RuntimeException ex) {
                getLog().warn("Failed to cancel subscription, skipping exception", ex);
            }
        }
    }
}
