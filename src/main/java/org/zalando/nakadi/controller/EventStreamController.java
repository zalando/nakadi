package org.zalando.nakadi.controller;

import com.codahale.metrics.Counter;
import com.codahale.metrics.MetricRegistry;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.servlet.mvc.method.annotation.StreamingResponseBody;
import org.zalando.nakadi.domain.Cursor;
import org.zalando.nakadi.domain.EventType;
import org.zalando.nakadi.exceptions.IllegalScopeException;
import org.zalando.nakadi.exceptions.InvalidCursorException;
import org.zalando.nakadi.exceptions.NakadiException;
import org.zalando.nakadi.exceptions.NoSuchEventTypeException;
import org.zalando.nakadi.repository.EventConsumer;
import org.zalando.nakadi.repository.EventTypeRepository;
import org.zalando.nakadi.repository.TopicRepository;
import org.zalando.nakadi.security.IClient;
import org.zalando.nakadi.service.ClosedConnectionsCrutch;
import org.zalando.nakadi.service.EventStream;
import org.zalando.nakadi.service.EventStreamConfig;
import org.zalando.nakadi.service.EventStreamFactory;
import org.zalando.problem.Problem;

import javax.annotation.Nullable;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.core.Response;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import static javax.ws.rs.core.Response.Status.BAD_REQUEST;
import static javax.ws.rs.core.Response.Status.FORBIDDEN;
import static javax.ws.rs.core.Response.Status.INTERNAL_SERVER_ERROR;
import static javax.ws.rs.core.Response.Status.NOT_FOUND;
import static javax.ws.rs.core.Response.Status.PRECONDITION_FAILED;
import static org.zalando.nakadi.metrics.MetricUtils.metricNameFor;

@RestController
public class EventStreamController {

    private static final Logger LOG = LoggerFactory.getLogger(EventStreamController.class);
    public static final String CONSUMERS_COUNT_METRIC_NAME = "consumers";

    private final EventTypeRepository eventTypeRepository;
    private final TopicRepository topicRepository;
    private final ObjectMapper jsonMapper;
    private final EventStreamFactory eventStreamFactory;
    private final MetricRegistry metricRegistry;
    private final ClosedConnectionsCrutch closedConnectionsCrutch;

    @Autowired
    public EventStreamController(final EventTypeRepository eventTypeRepository, final TopicRepository topicRepository,
                                 final ObjectMapper jsonMapper, final EventStreamFactory eventStreamFactory,
                                 final MetricRegistry metricRegistry, final ClosedConnectionsCrutch closedConnectionsCrutch) {

        this.eventTypeRepository = eventTypeRepository;
        this.topicRepository = topicRepository;
        this.jsonMapper = jsonMapper;
        this.eventStreamFactory = eventStreamFactory;
        this.metricRegistry = metricRegistry;
        this.closedConnectionsCrutch = closedConnectionsCrutch;
    }

    @RequestMapping(value = "/event-types/{name}/events", method = RequestMethod.GET)
    public StreamingResponseBody streamEvents(
            @PathVariable("name") final String eventTypeName,
            @Nullable @RequestParam(value = "batch_limit", required = false) final Integer batchLimit,
            @Nullable @RequestParam(value = "stream_limit", required = false) final Integer streamLimit,
            @Nullable @RequestParam(value = "batch_flush_timeout", required = false) final Integer batchTimeout,
            @Nullable @RequestParam(value = "stream_timeout", required = false) final Integer streamTimeout,
            @Nullable @RequestParam(value = "stream_keep_alive_limit", required = false) final Integer streamKeepAliveLimit,
            @Nullable @RequestHeader(name = "X-nakadi-cursors", required = false) final String cursorsStr,
            final HttpServletRequest request, final HttpServletResponse response, final IClient client) throws IOException {

        return outputStream -> {

            final AtomicBoolean connectionReady = closedConnectionsCrutch.listenForConnectionClose(request);
            Counter consumerCounter = null;
            EventConsumer eventConsumer = null;

            try {
                consumerCounter = metricRegistry.counter(metricNameFor(eventTypeName, CONSUMERS_COUNT_METRIC_NAME));
                consumerCounter.inc();

                @SuppressWarnings("UnnecessaryLocalVariable")
                final EventType eventType = eventTypeRepository.findByName(eventTypeName);
                final String topic = eventType.getTopic();

                client.authorize(eventType.getReadScopes());

                // validate parameters
                if (!topicRepository.topicExists(topic)) {
                    writeProblemResponse(response, outputStream, INTERNAL_SERVER_ERROR, "topic is absent in kafka");
                    return;
                }
                final EventStreamConfig.Builder builder = EventStreamConfig.builder()
                        .withTopic(topic)
                        .withBatchLimit(batchLimit)
                        .withStreamLimit(streamLimit)
                        .withBatchTimeout(batchTimeout)
                        .withStreamTimeout(streamTimeout)
                        .withStreamKeepAliveLimit(streamKeepAliveLimit);

                // deserialize cursors
                List<Cursor> cursors = null;
                if (cursorsStr != null) {
                    try {
                        cursors = jsonMapper.readValue(cursorsStr, new TypeReference<ArrayList<Cursor>>() {});
                    } catch (final IOException e) {
                        if (LOG.isDebugEnabled()) {
                            LOG.debug("Incorrect syntax of X-nakadi-cursors header: "  + cursorsStr + ". Respond with BAD_REQUEST.", e);
                        }
                        writeProblemResponse(response, outputStream, BAD_REQUEST,
                                "incorrect syntax of X-nakadi-cursors header");
                        return;
                    }
                }

                // if no cursors provided - read from the newest available events
                if (cursors == null) {
                    cursors = topicRepository
                            .listPartitions(topic)
                            .stream()
                            .map(pInfo -> new Cursor(pInfo.getPartitionId(), pInfo.getNewestAvailableOffset()))
                            .collect(Collectors.toList());
                }

                eventConsumer = topicRepository.createEventConsumer(topic, cursors);

                final Map<String, String> streamCursors = cursors
                        .stream()
                        .collect(Collectors.toMap(
                                Cursor::getPartition,
                                Cursor::getOffset));

                final EventStreamConfig streamConfig = builder
                        .withCursors(streamCursors)
                        .build();

                response.setStatus(HttpStatus.OK.value());
                response.setContentType("application/x-json-stream");
                final EventStream eventStream = eventStreamFactory.createEventStream(eventConsumer, outputStream,
                        streamConfig);

                outputStream.flush(); // Flush status code to client

                eventStream.streamEvents(connectionReady);
            } catch (final NoSuchEventTypeException e) {
                writeProblemResponse(response, outputStream, NOT_FOUND, "topic not found");
            } catch (final NakadiException e) {
                LOG.error("Error while trying to stream events. Respond with SERVICE_UNAVAILABLE.", e);
                writeProblemResponse(response, outputStream, e.asProblem());
            } catch (final InvalidCursorException e) {
                writeProblemResponse(response, outputStream, PRECONDITION_FAILED, e.getMessage());
            } catch (final IllegalScopeException e) {
                LOG.error("Error while trying to stream events. Respond with FORBIDDEN.", e);
                writeProblemResponse(response, outputStream, FORBIDDEN, e.getMessage());
            } catch (final Exception e) {
                LOG.error("Error while trying to stream events. Respond with INTERNAL_SERVER_ERROR.", e);
                writeProblemResponse(response, outputStream, INTERNAL_SERVER_ERROR, e.getMessage());
            } finally {
                connectionReady.set(false);
                if (consumerCounter != null) {
                    consumerCounter.dec();
                }
                if (eventConsumer != null) {
                    eventConsumer.close();
                }
                try {
                    outputStream.flush();
                } finally {
                    outputStream.close();
                }
            }
        };
    }

    private void writeProblemResponse(final HttpServletResponse response, final OutputStream outputStream,
                                      final Response.StatusType statusCode, final String message) throws IOException {
        writeProblemResponse(response, outputStream, Problem.valueOf(statusCode, message));
    }

    private void writeProblemResponse(final HttpServletResponse response, final OutputStream outputStream,
                                      final Problem problem) throws IOException {
        response.setStatus(problem.getStatus().getStatusCode());
        response.setContentType("application/problem+json");
        jsonMapper.writer().writeValue(outputStream, problem);
    }
}
