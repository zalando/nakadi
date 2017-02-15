package org.zalando.nakadi.service;

import com.google.common.collect.Lists;
import org.apache.kafka.common.KafkaException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zalando.nakadi.domain.ConsumedEvent;
import org.zalando.nakadi.domain.NakadiCursor;
import org.zalando.nakadi.repository.EventConsumer;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.Charset;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.zalando.nakadi.view.Cursor;

import static java.lang.System.currentTimeMillis;
import static java.util.function.Function.identity;

public class EventStream {

    private static final Logger LOG = LoggerFactory.getLogger(EventStream.class);
    public static final String BATCH_SEPARATOR = "\n";
    public static final Charset UTF8 = Charset.forName("UTF-8");

    private final OutputStream outputStream;
    private final EventConsumer eventConsumer;
    private final EventStreamConfig config;
    private final BlacklistService blacklistService;

    public EventStream(final EventConsumer eventConsumer,
                       final OutputStream outputStream,
                       final EventStreamConfig config,
                       final BlacklistService blacklistService) {
        this.eventConsumer = eventConsumer;
        this.outputStream = outputStream;
        this.config = config;
        this.blacklistService = blacklistService;
    }

    public void streamEvents(final AtomicBoolean connectionReady) {
        try {
            int messagesRead = 0;
            final Map<String, Integer> keepAliveInARow = createMapWithPartitionKeys(partition -> 0);

            final Map<String, List<String>> currentBatches =
                    createMapWithPartitionKeys(partition -> Lists.newArrayList());
            // Partition to NakadiCursor.
            final Map<String, NakadiCursor> latestOffsets = config.getCursors().stream().collect(
                    Collectors.toMap(NakadiCursor::getPartition, c -> c));

            final long start = currentTimeMillis();
            final Map<String, Long> batchStartTimes = createMapWithPartitionKeys(partition -> start);

            while (connectionReady.get() &&
                    !blacklistService.isConsumptionBlocked(config.getEtName(), config.getConsumingAppId())) {
                final Optional<ConsumedEvent> eventOrEmpty = eventConsumer.readEvent();

                if (eventOrEmpty.isPresent()) {
                    final ConsumedEvent event = eventOrEmpty.get();

                    // update offset for the partition of event that was read
                    latestOffsets.put(event.getPosition().getPartition(), event.getPosition());

                    // put message to batch
                    currentBatches.get(event.getPosition().getPartition()).add(event.getEvent());
                    messagesRead++;

                    // if we read the message - reset keep alive counter for this partition
                    keepAliveInARow.put(event.getPosition().getPartition(), 0);
                }

                // for each partition check if it's time to send the batch
                for (final String partition: latestOffsets.keySet()) {
                    final long timeSinceBatchStart = currentTimeMillis() - batchStartTimes.get(partition);
                    if (config.getBatchTimeout() * 1000 <= timeSinceBatchStart
                            || currentBatches.get(partition).size() >= config.getBatchLimit()) {

                        sendBatch(latestOffsets.get(partition), currentBatches.get(partition));

                        // if we hit keep alive count limit - close the stream
                        if (currentBatches.get(partition).size() == 0) {
                            keepAliveInARow.put(partition, keepAliveInARow.get(partition) + 1);
                        }

                        // init new batch for partition
                        currentBatches.get(partition).clear();
                        batchStartTimes.put(partition,currentTimeMillis());
                    }
                }

                // check if we reached keepAliveInARow for all the partitions; if yes - then close stream
                if (config.getStreamKeepAliveLimit() != 0) {
                    final boolean keepAliveLimitReachedForAllPartitions = keepAliveInARow
                            .values()
                            .stream()
                            .allMatch(keepAlives -> keepAlives >= config.getStreamKeepAliveLimit());

                    if (keepAliveLimitReachedForAllPartitions) {
                        break;
                    }
                }

                // check if we reached the stream timeout or message count limit
                final long timeSinceStart = currentTimeMillis() - start;
                if (config.getStreamTimeout() != 0 && timeSinceStart >= config.getStreamTimeout() * 1000
                        || config.getStreamLimit() != 0 && messagesRead >= config.getStreamLimit()) {

                    for (final String partition: latestOffsets.keySet()) {
                        if (currentBatches.get(partition).size() > 0) {
                            sendBatch(latestOffsets.get(partition), currentBatches.get(partition));
                        }
                    }

                    break;
                }
            }
        } catch (final IOException e) {
            LOG.info("I/O error occurred when streaming events (possibly client closed connection)", e);
        } catch (final IllegalStateException e) {
            LOG.info("Error occurred when streaming events (possibly server closed connection)", e);
        } catch (final KafkaException e) {
            LOG.error("Error occurred when polling events from kafka; consumer: {}, event-type: {}",
                    config.getConsumingAppId(), config.getEtName(), e);
        }
    }

    private <T> Map<String, T> createMapWithPartitionKeys(final Function<String, T> valueFunction) {
        return config
                .getCursors().stream().map(NakadiCursor::getPartition)
                .collect(Collectors.toMap(identity(), valueFunction));
    }

    public static String createStreamEvent(final Cursor cursor, final List<String> events) {
        final StringBuilder builder = new StringBuilder()
                .append("{\"cursor\":{\"partition\":\"").append(cursor.getPartition())
                .append("\",\"offset\":\"").append(cursor.getOffset()).append("\"}");
        if (!events.isEmpty()) {
            builder.append(",\"events\":[");
            events.forEach(event -> builder.append(event).append(","));
            builder.deleteCharAt(builder.length() - 1).append("]");
        }

        builder.append("}").append(BATCH_SEPARATOR);

        return builder.toString();
    }

    private void sendBatch(final NakadiCursor topicPosition, final List<String> currentBatch)
            throws IOException {
        // create stream event batch for current partition and send it; if there were
        // no events, it will be just a keep-alive
        final String streamEvent = createStreamEvent(Cursor.fromTopicPosition(topicPosition), currentBatch);
        outputStream.write(streamEvent.getBytes(UTF8));
        outputStream.flush();
    }

    public void close() throws IOException {
        this.eventConsumer.close();
    }

}
