package de.zalando.aruha.nakadi.service;

import static java.lang.System.currentTimeMillis;

import static java.util.function.Function.identity;

import java.io.IOException;
import java.io.OutputStream;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import de.zalando.aruha.nakadi.NakadiException;
import de.zalando.aruha.nakadi.domain.ConsumedEvent;
import de.zalando.aruha.nakadi.repository.EventConsumer;

public class EventStream {

    private static final Logger LOG = LoggerFactory.getLogger(EventStream.class);

    private static final String BATCH_SEPARATOR = "\n";

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
//            int keepAliveInARow = 0;
            int messagesRead = 0;

            Map<String, List<String>> currentBatches = config
                    .getCursors()
                    .keySet()
                    .stream()
                    .collect(Collectors.toMap(
                            identity(),
                            partition -> Lists.newArrayList()));
            final Map<String, String> latestOffsets = Maps.newHashMap(config.getCursors());

            long start = currentTimeMillis();
            Map<String, Long> batchStartTimes = config
                    .getCursors()
                    .keySet()
                    .stream()
                    .collect(Collectors.toMap(
                            identity(),
                            partition -> start));

            while (true) {
                final Optional<ConsumedEvent> eventOrEmpty = eventConsumer.readEvent();

                if (eventOrEmpty.isPresent()) {
                    final ConsumedEvent event = eventOrEmpty.get();

                    // update offset for the partition of event that was read
                    latestOffsets.put(event.getPartition(), event.getNextOffset());

                    // put message to batch
                    currentBatches.get(event.getPartition()).add(event.getEvent());
                    messagesRead++;

                    // if we read the message - reset keep alive counter
                    //keepAliveInARow = 0;
                }

                // for each partition check if it's time to send the batch
                for (final String partition: config.getCursors().keySet()) {
                    long timeSinceBatchStart = currentTimeMillis() - batchStartTimes.get(partition);
                    if (config.getBatchTimeout() * 1000 <= timeSinceBatchStart
                            || currentBatches.get(partition).size() >= config.getBatchLimit()) {

                        sendBatch(partition, latestOffsets.get(partition), currentBatches.get(partition));

                        // if we hit keep alive count limit - close the stream
//                        if (messagesReadInBatch == 0) {
//                            if (config.getBatchKeepAliveLimit().isPresent()
//                                    && keepAliveInARow >= config.getBatchKeepAliveLimit().get()) {
//                                break;
//                            }
//
//                            keepAliveInARow++;
//                        }

                        // init new batch for partition
                        currentBatches.get(partition).clear();;
                        batchStartTimes.put(partition,currentTimeMillis());
                    }
                }

                // check if we reached the stream timeout or message count limit
                long timeSinceStart = currentTimeMillis() - start;
                if (config.getStreamTimeout().isPresent() && timeSinceStart >= config.getStreamTimeout().get() * 1000
                        || config.getStreamLimit().isPresent() && messagesRead >= config.getStreamLimit().get()) {

                    for (final String partition: config.getCursors().keySet()) {
                        if (currentBatches.get(partition).size() > 0) {
                            sendBatch(partition, latestOffsets.get(partition), currentBatches.get(partition));
                        }
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

    private String createStreamEvent(final String partition, final String offset, final List<String> events,
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
        outputStream.write(streamEvent.getBytes());
        outputStream.flush();
    }

    /*private void sendBatch(final Map<String, String> latestOffsets, final Map<String, List<String>> currentBatch)
        throws IOException {

        // iterate through all partitions we stream for
        for (String partition : config.getCursors().keySet()) {
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
    }*/

}
