package de.zalando.aruha.nakadi.controller;

import com.codahale.metrics.annotation.Timed;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import de.zalando.aruha.nakadi.NakadiException;
import de.zalando.aruha.nakadi.domain.Cursor;
import de.zalando.aruha.nakadi.domain.Problem;
import de.zalando.aruha.nakadi.repository.EventConsumer;
import de.zalando.aruha.nakadi.repository.TopicRepository;
import de.zalando.aruha.nakadi.service.EventStream;
import de.zalando.aruha.nakadi.service.EventStreamConfig;
import de.zalando.aruha.nakadi.utils.FlushableGZIPOutputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.servlet.mvc.method.annotation.StreamingResponseBody;

import javax.annotation.Nullable;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

@RestController
public class EventStreamController {

    private static final Logger LOG = LoggerFactory.getLogger(EventStreamController.class);

    private TopicRepository topicRepository;

    private ObjectMapper jsonMapper;

    public EventStreamController(final TopicRepository topicRepository, final ObjectMapper jsonMapper) {
        this.topicRepository = topicRepository;
        this.jsonMapper = jsonMapper;
    }

    @Timed(name = "stream_events_for_event_type", absolute = true)
    @RequestMapping(value = "/event-types/{name}/events", method = RequestMethod.GET)
    public StreamingResponseBody streamEventsFromPartition(
            @PathVariable("name") final String eventTypeName,
            @RequestParam(value = "batch_limit", required = false, defaultValue = "1") final Integer batchLimit,
            @Nullable @RequestParam(value = "stream_limit", required = false) final Integer streamLimit,
            @RequestParam(value = "batch_flush_timeout", required = false, defaultValue = "30") final Integer batchTimeout,
            @Nullable @RequestParam(value = "stream_timeout", required = false) final Integer streamTimeout,
            @Nullable @RequestParam(value = "batch_keep_alive_limit", required = false) final Integer batchKeepAliveLimit,
            @Nullable @RequestHeader(name = "X-Flow-Id", required = false) final String flowId,
            @Nullable @RequestHeader(name = "X-nakadi-cursors", required = false) final String cursorsStr,
            final HttpServletRequest request, final HttpServletResponse response) throws IOException {

        LOG.trace("starting event stream for flow id: {}", flowId);

        return outputStream -> {
            try {
                // todo: we should get topic from EventType after persistence of EventType is implemented
                @SuppressWarnings("UnnecessaryLocalVariable")
                final String topic = eventTypeName;

                // valudate parameters
                if (!topicRepository.topicExists(topic)) {
                    writeProblemResponse(response, outputStream, HttpStatus.NOT_FOUND.value(),
                            new Problem("topic not found"));
                    return;
                }
                if (streamLimit != null && streamLimit < batchLimit) {
                    writeProblemResponse(response, outputStream, HttpStatus.UNPROCESSABLE_ENTITY.value(),
                            new Problem("stream_limit can't be lower than batch_limit"));
                    return;
                }
                if (streamTimeout != null && streamTimeout < batchTimeout) {
                    writeProblemResponse(response, outputStream, HttpStatus.UNPROCESSABLE_ENTITY.value(),
                            new Problem("stream_timeout can't be lower than batch_flush_timeout"));
                    return;
                }

                // deserialize cursors
                Optional<List<Cursor>> cursors = Optional.empty();
                if (cursorsStr != null) {
                    try {
                        cursors = Optional.of(jsonMapper.<List<Cursor>>readValue(cursorsStr,
                                new TypeReference<ArrayList<Cursor>>() {}));
                    } catch (IOException e) {
                        writeProblemResponse(response, outputStream, HttpStatus.BAD_REQUEST.value(),
                                new Problem("incorrect syntax of X-nakadi-cursors header"));
                        return;
                    }
                }

                // check that offsets are not out of bounds
                if (cursors.isPresent()) {
                    if (!topicRepository.areCursorsCorrect(topic, cursors.get())) {
                        writeProblemResponse(response, outputStream, HttpStatus.UNPROCESSABLE_ENTITY.value(),
                                new Problem("cursors are not valid"));
                        return;
                    }
                }

                // convert cursors to map; if no cursors provided - read from the newest available events
                final Map<String, String> streamCursors = cursors
                        .orElseGet(() -> topicRepository
                                .listPartitions(topic)
                                .stream()
                                .map(pInfo -> new Cursor(pInfo.getPartitionId(), pInfo.getNewestAvailableOffset()))
                                .collect(Collectors.toList()))
                        .stream()
                        .collect(Collectors.toMap(
                                Cursor::getPartition,
                                Cursor::getOffset));

                final EventStreamConfig streamConfig = EventStreamConfig
                        .builder()
                        .withTopic(topic)
                        .withCursors(streamCursors)
                        .withBatchLimit(batchLimit)
                        .withStreamLimit(streamLimit)
                        .withBatchTimeout(batchTimeout)
                        .withStreamTimeout(streamTimeout)
                        .withBatchKeepAliveLimit(batchKeepAliveLimit)
                        .build();

                response.setStatus(HttpStatus.OK.value());

                final String acceptEncoding = request.getHeader("Accept-Encoding");
                final boolean gzipEnabled = acceptEncoding != null && acceptEncoding.contains("gzip");
                final OutputStream output = gzipEnabled ? new FlushableGZIPOutputStream(outputStream) : outputStream;

                if (gzipEnabled) {
                    response.addHeader("Content-Encoding", "gzip");
                }

                final EventConsumer eventConsumer = topicRepository.createEventConsumer(topic,
                        streamConfig.getCursors());
                final EventStream eventStream = new EventStream(eventConsumer, output, streamConfig);
                eventStream.streamEvents();

                if (gzipEnabled) {
                    output.close();
                }

            }
            catch (final NakadiException e) {
                writeProblemResponse(response, outputStream, HttpStatus.SERVICE_UNAVAILABLE.value(), e.asProblem());
            }
            catch (final Exception e) {
                writeProblemResponse(response, outputStream, HttpStatus.INTERNAL_SERVER_ERROR.value(),
                        new Problem(e.getMessage()));
            }
            finally {
                outputStream.flush();
                outputStream.close();
            }
        };
    }

    private void writeProblemResponse(final HttpServletResponse response, final OutputStream outputStream,
                                      final int statusCode, final Problem problem) throws IOException {
        response.setStatus(statusCode);
        jsonMapper.writer().writeValue(outputStream, problem);
    }
}
