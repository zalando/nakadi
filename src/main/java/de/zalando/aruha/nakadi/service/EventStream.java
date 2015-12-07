package de.zalando.aruha.nakadi.service;

import com.google.common.collect.ImmutableList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.servlet.mvc.method.annotation.ResponseBodyEmitter;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class EventStream implements Runnable {

    private static final Logger LOG = LoggerFactory.getLogger(EventStream.class);

    private static final String BATCH_SEPARATOR = "\n";

    private ResponseBodyEmitter responseEmitter;

    private EventStreamConfig config;

    public EventStream(final ResponseBodyEmitter responseEmitter, final EventStreamConfig config) {
        this.responseEmitter = responseEmitter;
        this.config = config;
    }

    @Override
    public void run() {
        try {
            while (true) {
                Thread.sleep(2000);
                responseEmitter.send("blah\n");
            }
        } catch (InterruptedException e) {
            LOG.error("Stream thread was interrupted while streaming", e);
        } catch (IOException e) {
            LOG.info("I/O error occured when streaming events (possibly client closed connection)", e);
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

    public void sendBatch(final Map<String, String> latestOffsets, final Map<String, List<String>> currentBatch) throws IOException {
        // iterate through all partitions we stream for
        for (String partition : config.getCursors().keySet()) {
            List<String> events = ImmutableList.of();

            // if there are some events read for current partitions - grab them
            if (currentBatch.get(partition) != null && !currentBatch.get(partition).isEmpty()) {
                events = currentBatch.get(partition);
            }

            // create stream event for current partition and send it; if there were no events, it will be just a keep-alive
            final String streamEvent = createStreamEvent(partition, latestOffsets.get(partition), events, Optional.empty());
            responseEmitter.send(streamEvent);
        }
    }

}
