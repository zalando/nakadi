package de.zalando.aruha.nakadi.controller;

import com.codahale.metrics.Counter;
import com.codahale.metrics.MetricRegistry;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import de.zalando.aruha.nakadi.domain.Cursor;
import de.zalando.aruha.nakadi.domain.CursorError;
import de.zalando.aruha.nakadi.domain.EventType;
import de.zalando.aruha.nakadi.exceptions.InvalidCursorException;
import de.zalando.aruha.nakadi.exceptions.NakadiException;
import de.zalando.aruha.nakadi.repository.EventConsumer;
import de.zalando.aruha.nakadi.repository.EventTypeRepository;
import de.zalando.aruha.nakadi.repository.TopicRepository;
import de.zalando.aruha.nakadi.service.EventStream;
import de.zalando.aruha.nakadi.service.EventStreamConfig;
import de.zalando.aruha.nakadi.service.EventStreamFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.context.request.NativeWebRequest;
import org.springframework.web.servlet.mvc.method.annotation.StreamingResponseBody;
import org.zalando.problem.Problem;

import javax.annotation.Nullable;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.core.Response;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static de.zalando.aruha.nakadi.metrics.MetricUtils.metricNameFor;
import static javax.ws.rs.core.Response.Status.BAD_REQUEST;
import static javax.ws.rs.core.Response.Status.INTERNAL_SERVER_ERROR;
import static javax.ws.rs.core.Response.Status.NOT_FOUND;
import static javax.ws.rs.core.Response.Status.PRECONDITION_FAILED;
import static javax.ws.rs.core.Response.Status.SERVICE_UNAVAILABLE;
import static org.zalando.problem.MoreStatus.UNPROCESSABLE_ENTITY;

@RestController
public class EventStreamController {

    private static final Logger LOG = LoggerFactory.getLogger(EventStreamController.class);
    public static final String CONSUMERS_COUNT_METRIC_NAME = "consumers";

    private final TopicRepository topicRepository;
    private final ObjectMapper jsonMapper;
    private final EventStreamFactory eventStreamFactory;
    private final MetricRegistry metricRegistry;
    private final EventTypeRepository eventTypeRepository;

    public EventStreamController(final TopicRepository topicRepository, final ObjectMapper jsonMapper,
                                 final EventStreamFactory eventStreamFactory, final MetricRegistry metricRegistry,
                                 EventTypeRepository eventTypeRepository)
    {
        this.topicRepository = topicRepository;
        this.jsonMapper = jsonMapper;
        this.eventStreamFactory = eventStreamFactory;
        this.metricRegistry = metricRegistry;
        this.eventTypeRepository = eventTypeRepository;
    }

    @RequestMapping(value = "/event-types/{name}/events", method = RequestMethod.GET)
    public StreamingResponseBody streamEvents(
            @PathVariable("name") final String eventTypeName,
            @RequestParam(value = "batch_limit", required = false, defaultValue = "1") final int batchLimit,
            @RequestParam(value = "stream_limit", required = false, defaultValue = "0") final int streamLimit,
            @RequestParam(value = "batch_flush_timeout", required = false, defaultValue = "30") final int batchTimeout,
            @RequestParam(value = "stream_timeout", required = false, defaultValue = "60") final int streamTimeout,
            @RequestParam(value = "stream_keep_alive_limit", required = false, defaultValue = "0") final int streamKeepAliveLimit,
            @Nullable @RequestHeader(name = "X-nakadi-cursors", required = false) final String cursorsStr,
            final NativeWebRequest request, final HttpServletResponse response) throws IOException {

        return outputStream -> {

            Counter consumerCounter = null;
            EventConsumer eventConsumer = null;

            try {
                consumerCounter = metricRegistry.counter(metricNameFor(eventTypeName, CONSUMERS_COUNT_METRIC_NAME));
                consumerCounter.inc();

                @SuppressWarnings("UnnecessaryLocalVariable")
                final String topic = eventTypeName;

                // validate parameters
                if (!topicRepository.topicExists(topic)) {
                    Optional<EventType> eventTypeO = eventTypeRepository.findByNameO(eventTypeName);
                    if (eventTypeO.isPresent()) {
                        writeProblemResponse(response, outputStream,INTERNAL_SERVER_ERROR, "topic is absent in kafka");
                        return;
                    }
                    writeProblemResponse(response, outputStream, NOT_FOUND, "topic not found");
                    return;
                }
                if (streamLimit != 0 && streamLimit < batchLimit) {
                    writeProblemResponse(response, outputStream, UNPROCESSABLE_ENTITY,
                            "stream_limit can't be lower than batch_limit");
                    return;
                }
                if (streamTimeout != 0 && streamTimeout < batchTimeout) {
                    writeProblemResponse(response, outputStream, UNPROCESSABLE_ENTITY,
                            "stream_timeout can't be lower than batch_flush_timeout");
                    return;
                }

                // deserialize cursors
                List<Cursor> cursors = null;
                if (cursorsStr != null) {
                    try {
                        cursors = jsonMapper.<List<Cursor>>readValue(cursorsStr,
                                new TypeReference<ArrayList<Cursor>>() {
                                });
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

                final EventStreamConfig streamConfig = new EventStreamConfig(topic, streamCursors, batchLimit,
                        streamLimit, batchTimeout, streamTimeout, streamKeepAliveLimit);

                response.setStatus(HttpStatus.OK.value());
                response.setContentType("application/x-json-stream");
                final EventStream eventStream = eventStreamFactory.createEventStream(eventConsumer, outputStream,
                        streamConfig);

                outputStream.flush(); // Flush status code to client

                eventStream.streamEvents();
            }
            catch (final NakadiException e) {
                LOG.error("Error while trying to stream events. Respond with SERVICE_UNAVAILABLE.", e);
                writeProblemResponse(response, outputStream, SERVICE_UNAVAILABLE, e.getProblemMessage());
            }
            catch (final InvalidCursorException e) {
                final String errorMessage = invalidCursorMessage(e.getError(), e.getCursor());
                writeProblemResponse(response, outputStream, PRECONDITION_FAILED, errorMessage);
            }
            catch (final Exception e) {
                LOG.error("Error while trying to stream events. Respond with INTERNAL_SERVER_ERROR.", e);
                writeProblemResponse(response, outputStream, INTERNAL_SERVER_ERROR, e.getMessage());
            }
            finally {
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

    private String invalidCursorMessage(final CursorError error, final Cursor cursor) {
        switch (error) {
            case PARTITION_NOT_FOUND:
                return "non existing partition " + cursor.getPartition();
            case EMPTY_PARTITION:
                return "partition " + cursor.getPartition() + " is empty";
            case UNAVAILABLE:
                return "offset " + cursor.getOffset() + " for partition " + cursor.getPartition() + " is unavailable";
            case NULL_OFFSET:
                return "offset must not be null";
            case NULL_PARTITION:
                return "partition must not be null";
            default:
                return "invalid offset " + cursor.getOffset() + " for partition " + cursor.getPartition();
        }
    }

    private void writeProblemResponse(final HttpServletResponse response, final OutputStream outputStream,
                                      final Response.StatusType statusCode, final String message) throws IOException {
        response.setStatus(statusCode.getStatusCode());
        response.setContentType("application/problem+json");
        jsonMapper.writer().writeValue(outputStream, Problem.valueOf(statusCode, message));
    }
}
