package de.zalando.aruha.nakadi.service;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import de.zalando.aruha.nakadi.NakadiException;
import de.zalando.aruha.nakadi.domain.ConsumedEvent;
import de.zalando.aruha.nakadi.domain.Cursor;
import de.zalando.aruha.nakadi.domain.TopicPartition;
import de.zalando.aruha.nakadi.repository.EventConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static de.zalando.aruha.nakadi.domain.TopicPartition.topicPartition;
import static java.lang.System.currentTimeMillis;
import static java.util.function.Function.identity;

public class EventStream {

    private static final Logger LOG = LoggerFactory.getLogger(EventStream.class);

    private static final String BATCH_SEPARATOR = "\n";

    private final OutputStream outputStream;

    protected final EventConsumer eventConsumer;

    private final EventStreamConfig config;

    private List<TopicPartition> partitions;

    private final List<Cursor> initialCursors;

    private AtomicReference<Optional<List<Cursor>>> newDistribution = new AtomicReference<>(Optional.empty());

    private String clientId;

    private String subscriptionId;

    public EventStream(final EventConsumer eventConsumer,
                       final OutputStream outputStream,
                       final EventStreamConfig config,
                       final List<Cursor> cursors) {
        this.eventConsumer = eventConsumer;
        this.outputStream = outputStream;
        this.config = config;
        initialCursors = cursors;
        partitions = cursorsToPartitions(cursors);
    }

    public void setOffsets(final List<Cursor> newDistribution) {
        this.newDistribution.lazySet(Optional.of(newDistribution));
    }

    public String getClientId() {
        return clientId;
    }

    public void setClientId(final String clientId) {
        this.clientId = clientId;
    }

    public String getSubscriptionId() {
        return subscriptionId;
    }

    public void setSubscriptionId(final String subscriptionId) {
        this.subscriptionId = subscriptionId;
    }

    public void streamEvents() {
        // todo: currently if:
        // - stream limit and timeout are not set AND
        // - the keep-alive is not set AND
        // - client closed the connection
        // => this thread can stuck till it tries to write to responseEmitter (which can be quite long).
        // So we should check somehow the status of connection in such cases, or forbid polling without keep-alive
        try {
            int keepAliveInARow = 0;
            int messagesRead = 0;
            int messagesReadInBatch = 0;

            Map<TopicPartition, List<String>> currentBatch = emptyBatch();
            Map<TopicPartition, String> latestOffsets = cursorsToOffsetMap(initialCursors);

            long start = currentTimeMillis();
            long batchStartTime = start;

            while (true) {

                // check if rebalance is invoked
                final Optional<List<Cursor>> newCursorsOrNone = newDistribution.getAndSet(Optional.empty());
                if (newCursorsOrNone.isPresent()) {
                    final List<Cursor> newCursors = newCursorsOrNone.get();
                    eventConsumer.setCursors(newCursors);
                    partitions = cursorsToPartitions(newCursors);
                    latestOffsets = cursorsToOffsetMap(newCursors);
                    // forget about all messages not flushed
                    messagesReadInBatch = 0;
                    currentBatch = emptyBatch();
                    batchStartTime = currentTimeMillis();
                }

                final Optional<ConsumedEvent> eventOrEmpty = eventConsumer.readEvent();

                if (eventOrEmpty.isPresent()) {
                    final ConsumedEvent event = eventOrEmpty.get();

                    // update offset for the partition of event that was read
                    latestOffsets.put(event.getTopicPartition(), event.getNextOffset());

                    // put message to batch
                    currentBatch.get(event.getTopicPartition()).add(event.getEvent());
                    messagesRead++;
                    messagesReadInBatch++;

                    // if we read the message - reset keep alive counter
                    keepAliveInARow = 0;
                }

                // check if it's time to send the batch
                long timeSinceBatchStart = currentTimeMillis() - batchStartTime;
                if (config.getBatchTimeout().isPresent() && config.getBatchTimeout().get() * 1000 <= timeSinceBatchStart
                        || messagesReadInBatch >= config.getBatchLimit()) {

                    sendBatch(latestOffsets, currentBatch);

                    // if we hit keep alive count limit - close the stream
                    if (messagesReadInBatch == 0) {
                        if (config.getBatchKeepAliveLimit().isPresent()
                                && keepAliveInARow >= config.getBatchKeepAliveLimit().get()) {
                            break;
                        }

                        keepAliveInARow++;
                    }

                    // init new batch
                    messagesReadInBatch = 0;
                    currentBatch = emptyBatch();
                    batchStartTime = currentTimeMillis();

                    outputStream.write(BATCH_SEPARATOR.getBytes());
                    outputStream.flush();
                }

                // check if we reached the stream timeout or message count limit
                long timeSinceStart = currentTimeMillis() - start;
                if (config.getStreamTimeout().isPresent() && timeSinceStart >= config.getStreamTimeout().get() * 1000
                        || config.getStreamLimit().isPresent() && messagesRead >= config.getStreamLimit().get()) {

                    if (messagesReadInBatch > 0) {
                        sendBatch(latestOffsets, currentBatch);
                    }

                    break;
                }
            }
        } catch (IOException e) {
            LOG.info("I/O error occurred when streaming events (possibly client closed connection)", e);
        } catch (IllegalStateException e) {
            LOG.info("Error occurred when streaming events (possibly server closed connection)", e);
        } catch (NakadiException e) {
            LOG.error("Error occurred when polling events from kafka", e);
        }
    }

    private Map<TopicPartition, List<String>> emptyBatch() {
        return partitions.stream().collect(Collectors.toMap(identity(), partition -> Lists.newArrayList()));
    }

    private String createStreamEvent(final TopicPartition partition, final String offset, final List<String> events,
            final Optional<String> topology) {
        final StringBuilder builder = new StringBuilder()
                .append("{\"cursor\":{")
                .append("\"topic\":\"").append(partition.getTopic()).append("\",")
                .append("\"partition\":\"").append(partition.getPartition()).append("\",")
                .append("\"offset\":\"").append(offset).append("\"}");
        if (!events.isEmpty()) {
            builder.append(",\"events\":[");
            events.stream().forEach(event -> builder.append(event).append(","));
            builder.deleteCharAt(builder.length() - 1).append("]");
        }

        builder.append("}").append(BATCH_SEPARATOR);
        return builder.toString();
    }

    private void sendBatch(final Map<TopicPartition, String> latestOffsets, final Map<TopicPartition, List<String>> currentBatch)
        throws IOException {

        // iterate through all partitions we stream for
        for (TopicPartition partition : partitions) {
            List<String> events = ImmutableList.of();

            // if there are some events read for current partition - grab them
            if (currentBatch.get(partition) != null && !currentBatch.get(partition).isEmpty()) {
                events = currentBatch.get(partition);
            }

            // create stream event for current partition and send it; if there were no events, it will be just a
            // keep-alive
            final String streamEvent = createStreamEvent(partition, latestOffsets.get(partition), events,
                    Optional.empty());
            outputStream.write(streamEvent.getBytes());
        }
    }

    private Map<TopicPartition, String> cursorsToOffsetMap(final List<Cursor> cursors) {
        return cursors
                .stream()
                .collect(Collectors.toMap(
                        c -> topicPartition(c.getTopic(), c.getPartition()),
                        Cursor::getOffset
                ));
    }

    private List<TopicPartition> cursorsToPartitions(final List<Cursor> cursors) {
        return cursors
                .stream()
                .map(c -> topicPartition(c.getTopic(), c.getPartition()))
                .collect(Collectors.toList());
    }

}
