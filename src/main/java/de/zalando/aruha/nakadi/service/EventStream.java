package de.zalando.aruha.nakadi.service;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import de.zalando.aruha.nakadi.domain.ConsumedEvent;
import de.zalando.aruha.nakadi.repository.EventConsumer;
import org.apache.kafka.common.KafkaException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.Charset;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

import static java.lang.System.currentTimeMillis;
import static java.util.function.Function.identity;

public class EventStream {

    private static final Logger LOG = LoggerFactory.getLogger(EventStream.class);

    public static final String BATCH_SEPARATOR = "\n";
    public static final Charset UTF8 = Charset.forName("UTF-8");

    private final OutputStream outputStream;

    private final EventConsumer eventConsumer;

    private final EventStreamConfig config;

    public EventStream(final EventConsumer eventConsumer, final OutputStream outputStream,
            final EventStreamConfig config) {
        this.eventConsumer = eventConsumer;
        this.outputStream = outputStream;
        this.config = config;
    }

    public void streamEvents() {
        try {
            int messagesRead = 0;
            final Map<String, Integer> keepAliveInARow = createMapWithPartitionKeys(partition -> 0);

            final Map<String, List<String>> currentBatches =
                    createMapWithPartitionKeys(partition -> Lists.newArrayList());
            final Map<String, String> latestOffsets = Maps.newHashMap(config.getCursors());

            final long start = currentTimeMillis();
            final Map<String, Long> batchStartTimes = createMapWithPartitionKeys(partition -> start);

            while (true) {
                final Optional<ConsumedEvent> eventOrEmpty = eventConsumer.readEvent();

                if (eventOrEmpty.isPresent()) {
                    final ConsumedEvent event = eventOrEmpty.get();

                    // update offset for the partition of event that was read
                    latestOffsets.put(event.getPartition(), event.getOffset());

                    // put message to batch
                    currentBatches.get(event.getPartition()).add(event.getEvent());
                    messagesRead++;

                    // if we read the message - reset keep alive counter for this partition
                    keepAliveInARow.put(event.getPartition(), 0);
                }

                // for each partition check if it's time to send the batch
                for (final String partition: config.getCursors().keySet()) {
                    final long timeSinceBatchStart = currentTimeMillis() - batchStartTimes.get(partition);
                    if (config.getBatchTimeout() * 1000 <= timeSinceBatchStart
                            || currentBatches.get(partition).size() >= config.getBatchLimit()) {

                        sendBatch(partition, latestOffsets.get(partition), currentBatches.get(partition));

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

                    for (final String partition: config.getCursors().keySet()) {
                        if (currentBatches.get(partition).size() > 0) {
                            sendBatch(partition, latestOffsets.get(partition), currentBatches.get(partition));
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
            LOG.error("Error occurred when polling events from kafka", e);
        }
    }

    private <T> Map<String, T> createMapWithPartitionKeys(final Function<String, T> valueFunction) {
        return config
                .getCursors()
                .keySet()
                .stream()
                .collect(Collectors.toMap(identity(), valueFunction));
    }

    public static String createStreamEvent(final String partition, final String offset, final List<String> events,
            final Optional<String> topology) {
        final StringBuilder builder = new StringBuilder().append("{\"cursor\":{\"partition\":\"").append(partition)
                                                         .append("\",\"offset\":\"").append(offset).append("\"}");
        if (!events.isEmpty()) {
            builder.append(",\"events\":[");
            events.stream().forEach(event -> builder.append(event).append(","));
            builder.deleteCharAt(builder.length() - 1).append("]");
        }

        builder.append("}").append(BATCH_SEPARATOR);
        return builder.toString();
    }

    private void sendBatch(final String partition, final String offset, final List<String> currentBatch)
            throws IOException {
        // create stream event batch for current partition and send it; if there were
        // no events, it will be just a keep-alive
        final String streamEvent = createStreamEvent(partition, offset, currentBatch, Optional.empty());
        outputStream.write(streamEvent.getBytes(UTF8));
        outputStream.flush();
    }

}
