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
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

import static java.lang.System.currentTimeMillis;
import static java.util.function.Function.identity;

public class EventStream {

    private static final Logger LOG = LoggerFactory.getLogger(EventStream.class);

    private final OutputStream outputStream;
    private final EventConsumer eventConsumer;
    private final EventStreamConfig config;
    private final CursorConverter cursorConverter;
    private final Meter bytesFlushedMeter;
    private final EventStreamWriter eventStreamWriter;
    private final ConsumptionKpiCollector kpiCollector;
    private final EventStreamChecks eventStreamChecks;

    public EventStream(final EventConsumer eventConsumer,
                       final OutputStream outputStream,
                       final EventStreamConfig config,
                       final EventStreamChecks eventStreamChecks,
                       final CursorConverter cursorConverter,
                       final Meter bytesFlushedMeter,
                       final EventStreamWriter eventStreamWriter,
                       final ConsumptionKpiCollector kpiCollector) {
        this.eventConsumer = eventConsumer;
        this.outputStream = outputStream;
        this.config = config;
        this.cursorConverter = cursorConverter;
        this.bytesFlushedMeter = bytesFlushedMeter;
        this.eventStreamWriter = eventStreamWriter;
        this.eventStreamChecks = eventStreamChecks;
        this.kpiCollector = kpiCollector;
    }

    public void streamEvents(final Runnable checkAuthorization) {
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

            long bytesInMemory = 0;

            while (true) {

                if (eventStreamChecks.isConsumptionBlocked(
                        Collections.singleton(config.getEtName()),
                        config.getConsumingClient().getClientId())) {
                    break;
                }

                checkAuthorization.run();

                if (consumedEvents.isEmpty()) {
                    final List<ConsumedEvent> eventsFromKafka = eventConsumer.readEvents();
                    for (final ConsumedEvent evt : eventsFromKafka) {
                        if (eventStreamChecks.isConsumptionBlocked(evt)) {
                            continue;
                        }
                        consumedEvents.add(evt);
                    }
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
                    bytesInMemory += event.getEvent().length;

                    // if we read the message - reset keep alive counter for this partition
                    keepAliveInARow.put(event.getPosition().getPartition(), 0);
                }

                // for each partition check if it's time to send the batch
                for (final String partition : latestOffsets.keySet()) {
                    final long timeSinceBatchStart = currentTimeMillis() - batchStartTimes.get(partition);
                    if (config.getBatchTimeout() * 1000L <= timeSinceBatchStart
                            || currentBatches.get(partition).size() >= config.getBatchLimit()) {
                        final List<byte[]> eventsToSend = currentBatches.get(partition);
                        sendBatch(latestOffsets.get(partition), eventsToSend);

                        if (!eventsToSend.isEmpty()) {
                            bytesInMemory -= eventsToSend.stream().mapToLong(v -> v.length).sum();
                            eventsToSend.clear();
                        } else {
                            // if we hit keep alive count limit - close the stream
                            keepAliveInARow.put(partition, keepAliveInARow.get(partition) + 1);
                        }

                        batchStartTimes.put(partition, currentTimeMillis());
                    }
                }
                // Dump some data that is exceeding memory limits
                while (isMemoryLimitReached(bytesInMemory)) {
                    final Map.Entry<String, List<byte[]>> heaviestPartition = currentBatches.entrySet().stream()
                            .max(Comparator.comparing(
                                    entry -> entry.getValue().stream().mapToLong(event -> event.length).sum()))
                            .get();
                    sendBatch(latestOffsets.get(heaviestPartition.getKey()), heaviestPartition.getValue());
                    final long freed = heaviestPartition.getValue().stream().mapToLong(v -> v.length).sum();
                    LOG.info("Memory limit reached for event type {}: {} bytes. Freed: {} bytes, {} messages",
                            config.getEtName(), bytesInMemory, freed, heaviestPartition.getValue().size());
                    bytesInMemory -= freed;
                    // Init new batch for subscription
                    heaviestPartition.getValue().clear();
                    batchStartTimes.put(heaviestPartition.getKey(), currentTimeMillis());
                }

                kpiCollector.checkAndSendKpi();

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
                if (config.getStreamTimeout() != 0 && timeSinceStart >= config.getStreamTimeout() * 1000L
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
                    config.getConsumingClient().getClientId(), config.getEtName(), e);
        } finally {
            kpiCollector.sendKpi();
        }
    }

    private boolean isMemoryLimitReached(final long memoryUsed) {
        return memoryUsed > config.getMaxMemoryUsageBytes();
    }

    private <T> Map<String, T> createMapWithPartitionKeys(final Function<String, T> valueFunction) {
        return config
                .getCursors().stream().map(NakadiCursor::getPartition)
                .collect(Collectors.toMap(identity(), valueFunction));
    }

    private void sendBatch(final NakadiCursor topicPosition, final List<byte[]> currentBatch)
            throws IOException {
        final long bytesWritten = eventStreamWriter
                .writeBatch(outputStream, cursorConverter.convert(topicPosition), currentBatch);
        bytesFlushedMeter.mark(bytesWritten);
        kpiCollector.recordBatchSent(config.getEtName(), bytesWritten, currentBatch.size());
    }


    public void close() throws IOException {
        this.eventConsumer.close();
    }

}
