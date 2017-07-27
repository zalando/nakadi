package org.zalando.nakadi.service;

import com.codahale.metrics.Meter;
import com.google.common.collect.Lists;
import org.apache.kafka.common.KafkaException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zalando.nakadi.domain.ConsumedEvent;
import org.zalando.nakadi.domain.NakadiCursor;
import org.zalando.nakadi.repository.EventConsumer;

import java.io.IOException;
import java.io.OutputStream;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import java.util.stream.Collectors;

import static java.lang.System.currentTimeMillis;
import static java.util.function.Function.identity;

public class EventStream {

    private static final Logger LOG = LoggerFactory.getLogger(EventStream.class);

    private final OutputStream outputStream;
    private final EventConsumer eventConsumer;
    private final EventStreamConfig config;
    private final BlacklistService blacklistService;
    private final CursorConverter cursorConverter;
    private final Meter bytesFlushedMeter;
    private final EventStreamWriterProvider writer;

    public EventStream(final EventConsumer eventConsumer,
                       final OutputStream outputStream,
                       final EventStreamConfig config,
                       final BlacklistService blacklistService,
                       final CursorConverter cursorConverter, final Meter bytesFlushedMeter,
                       final EventStreamWriterProvider writer) {
        this.eventConsumer = eventConsumer;
        this.outputStream = outputStream;
        this.config = config;
        this.blacklistService = blacklistService;
        this.cursorConverter = cursorConverter;
        this.bytesFlushedMeter = bytesFlushedMeter;
        this.writer = writer;
    }

    public void streamEvents(final AtomicBoolean connectionReady, final Runnable checkAuthorization) {
        try {
            int messagesRead = 0;
            final Map<String, Integer> keepAliveInARow = createMapWithPartitionKeys(partition -> 0);

            final Map<String, List<byte[]>> currentBatches =
                    createMapWithPartitionKeys(partition -> Lists.newArrayList());
            // Partition to NakadiCursor.
            final Map<String, NakadiCursor> latestOffsets = config.getCursors().stream().collect(
                    Collectors.toMap(NakadiCursor::getPartition, c -> c));

            final long start = currentTimeMillis();
            final Map<String, Long> batchStartTimes = createMapWithPartitionKeys(partition -> start);
            final List<ConsumedEvent> consumedEvents = new LinkedList<>();
            while (connectionReady.get() &&
                    !blacklistService.isConsumptionBlocked(config.getEtName(), config.getConsumingAppId())) {

                checkAuthorization.run();

                if (consumedEvents.isEmpty()) {
                    // TODO: There are a lot of optimizations here, one can significantly improve code by processing
                    // all events at the same time, instead of processing one by one.
                    consumedEvents.addAll(eventConsumer.readEvents());
                }
                final Optional<ConsumedEvent> eventOrEmpty = consumedEvents.isEmpty() ?
                        Optional.empty() : Optional.of(consumedEvents.remove(0));

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
                for (final String partition : latestOffsets.keySet()) {
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
                        batchStartTimes.put(partition, currentTimeMillis());
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

                    for (final String partition : latestOffsets.keySet()) {
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

    private void sendBatch(final NakadiCursor topicPosition, final List<byte[]> currentBatch)
            throws IOException {
        final int bytesWritten = writer.getWriter()
                .writeBatch(outputStream, cursorConverter.convert(topicPosition), currentBatch);
        bytesFlushedMeter.mark(bytesWritten);
    }


    public void close() throws IOException {
        this.eventConsumer.close();
    }

}
