package org.zalando.nakadi.controller;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.core.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.servlet.mvc.method.annotation.StreamingResponseBody;
import org.zalando.nakadi.domain.CursorError;
import org.zalando.nakadi.domain.EventType;
import org.zalando.nakadi.domain.NakadiCursor;
import org.zalando.nakadi.domain.PartitionStatistics;
import org.zalando.nakadi.domain.Timeline;
import org.zalando.nakadi.exceptions.IllegalScopeException;
import org.zalando.nakadi.exceptions.InternalNakadiException;
import org.zalando.nakadi.exceptions.InvalidCursorException;
import org.zalando.nakadi.exceptions.NakadiException;
import org.zalando.nakadi.exceptions.NoConnectionSlotsException;
import org.zalando.nakadi.exceptions.NoSuchEventTypeException;
import org.zalando.nakadi.exceptions.ServiceUnavailableException;
import org.zalando.nakadi.exceptions.UnparseableCursorException;
import org.zalando.nakadi.metrics.MetricUtils;
import org.zalando.nakadi.repository.EventConsumer;
import org.zalando.nakadi.repository.EventTypeRepository;
import org.zalando.nakadi.repository.TopicRepository;
import org.zalando.nakadi.security.Client;
import org.zalando.nakadi.service.BlacklistService;
import org.zalando.nakadi.service.ClosedConnectionsCrutch;
import org.zalando.nakadi.service.ConnectionSlot;
import org.zalando.nakadi.service.ConsumerLimitingService;
import org.zalando.nakadi.service.CursorConverter;
import org.zalando.nakadi.service.EventStream;
import org.zalando.nakadi.service.EventStreamConfig;
import org.zalando.nakadi.service.EventStreamFactory;
import org.zalando.nakadi.service.timeline.TimelineService;
import org.zalando.nakadi.util.FeatureToggleService;
import org.zalando.nakadi.view.Cursor;
import org.zalando.problem.Problem;
import static javax.ws.rs.core.Response.Status.BAD_REQUEST;
import static javax.ws.rs.core.Response.Status.FORBIDDEN;
import static javax.ws.rs.core.Response.Status.INTERNAL_SERVER_ERROR;
import static javax.ws.rs.core.Response.Status.NOT_FOUND;
import static javax.ws.rs.core.Response.Status.PRECONDITION_FAILED;
import static org.zalando.nakadi.metrics.MetricUtils.metricNameFor;
import static org.zalando.nakadi.util.FeatureToggleService.Feature.LIMIT_CONSUMERS_NUMBER;

@RestController
public class EventStreamController {

    private static final Logger LOG = LoggerFactory.getLogger(EventStreamController.class);
    public static final String CONSUMERS_COUNT_METRIC_NAME = "consumers";

    private final EventTypeRepository eventTypeRepository;
    private final TimelineService timelineService;
    private final ObjectMapper jsonMapper;
    private final EventStreamFactory eventStreamFactory;
    private final MetricRegistry metricRegistry;
    private final ClosedConnectionsCrutch closedConnectionsCrutch;
    private final BlacklistService blacklistService;
    private final ConsumerLimitingService consumerLimitingService;
    private final FeatureToggleService featureToggleService;
    private final CursorConverter cursorConverter;
    private final MetricRegistry streamMetrics;

    @Autowired
    public EventStreamController(final EventTypeRepository eventTypeRepository,
                                 final TimelineService timelineService,
                                 final ObjectMapper jsonMapper,
                                 final EventStreamFactory eventStreamFactory,
                                 final MetricRegistry metricRegistry,
                                 @Qualifier("streamMetricsRegistry") final MetricRegistry streamMetrics,
                                 final ClosedConnectionsCrutch closedConnectionsCrutch,
                                 final BlacklistService blacklistService,
                                 final ConsumerLimitingService consumerLimitingService,
                                 final FeatureToggleService featureToggleService,
                                 final CursorConverter cursorConverter) {
        this.eventTypeRepository = eventTypeRepository;
        this.timelineService = timelineService;
        this.jsonMapper = jsonMapper;
        this.eventStreamFactory = eventStreamFactory;
        this.metricRegistry = metricRegistry;
        this.streamMetrics = streamMetrics;
        this.closedConnectionsCrutch = closedConnectionsCrutch;
        this.blacklistService = blacklistService;
        this.consumerLimitingService = consumerLimitingService;
        this.featureToggleService = featureToggleService;
        this.cursorConverter = cursorConverter;
    }

    @VisibleForTesting
    List<NakadiCursor> getStreamingStart(final EventType eventType, final String cursorsStr)
            throws UnparseableCursorException, ServiceUnavailableException, InvalidCursorException,
            InternalNakadiException, NoSuchEventTypeException {
        List<Cursor> cursors = null;
        if (cursorsStr != null) {
            try {
                cursors = jsonMapper.readValue(cursorsStr, new TypeReference<ArrayList<Cursor>>() {
                });
            } catch (final IOException ex) {
                throw new UnparseableCursorException("incorrect syntax of X-nakadi-cursors header", ex, cursorsStr);
            }
        }
        final Timeline latestTimeline = timelineService.getTimeline(eventType);
        final TopicRepository latestTopicRepository = timelineService.getTopicRepository(latestTimeline);
        if (null != cursors) {
            final List<NakadiCursor> result = new ArrayList<>();
            for (final Cursor c : cursors) {
                result.add(cursorConverter.convert(eventType.getName(), c));
            }
            if (result.isEmpty()) {
                throw new InvalidCursorException(CursorError.INVALID_FORMAT);
            }
            return result;
        } else {
            // if no cursors provided - read from the newest available events
            return latestTopicRepository.loadTopicStatistics(Collections.singletonList(latestTimeline))
                    .stream()
                    .map(PartitionStatistics::getLast)
                    .collect(Collectors.toList());
        }
    }

    @RequestMapping(value = "/event-types/{name}/events", method = RequestMethod.GET)
    public StreamingResponseBody streamEvents(
            @PathVariable("name") final String eventTypeName,
            @Nullable @RequestParam(value = "batch_limit", required = false) final Integer batchLimit,
            @Nullable @RequestParam(value = "stream_limit", required = false) final Integer streamLimit,
            @Nullable @RequestParam(value = "batch_flush_timeout", required = false) final Integer batchTimeout,
            @Nullable @RequestParam(value = "stream_timeout", required = false) final Integer streamTimeout,
            @Nullable
            @RequestParam(value = "stream_keep_alive_limit", required = false) final Integer streamKeepAliveLimit,
            @Nullable @RequestHeader(name = "X-nakadi-cursors", required = false) final String cursorsStr,
            final HttpServletRequest request, final HttpServletResponse response, final Client client)
            throws IOException {

        return outputStream -> {

            if (blacklistService.isConsumptionBlocked(eventTypeName, client.getClientId())) {
                writeProblemResponse(response, outputStream,
                        Problem.valueOf(Response.Status.FORBIDDEN, "Application or event type is blocked"));
                return;
            }

            final AtomicBoolean connectionReady = closedConnectionsCrutch.listenForConnectionClose(request);
            Counter consumerCounter = null;
            EventStream eventStream = null;
            List<ConnectionSlot> connectionSlots = ImmutableList.of();

            try {
                @SuppressWarnings("UnnecessaryLocalVariable")
                final EventType eventType = eventTypeRepository.findByName(eventTypeName);

                client.checkScopes(eventType.getReadScopes());

                // validate parameters
                final EventStreamConfig streamConfig = EventStreamConfig.builder()
                        .withBatchLimit(batchLimit)
                        .withStreamLimit(streamLimit)
                        .withBatchTimeout(batchTimeout)
                        .withStreamTimeout(streamTimeout)
                        .withStreamKeepAliveLimit(streamKeepAliveLimit)
                        .withEtName(eventTypeName)
                        .withConsumingAppId(client.getClientId())
                        .withCursors(getStreamingStart(eventType, cursorsStr))
                        .build();

                // acquire connection slots to limit the number of simultaneous connections from one client
                if (featureToggleService.isFeatureEnabled(LIMIT_CONSUMERS_NUMBER)) {
                    final List<String> partitions = streamConfig.getCursors().stream()
                            .map(NakadiCursor::getPartition)
                            .collect(Collectors.toList());
                    connectionSlots = consumerLimitingService.acquireConnectionSlots(
                            client.getClientId(), eventTypeName, partitions);
                }

                consumerCounter = metricRegistry.counter(metricNameFor(eventTypeName, CONSUMERS_COUNT_METRIC_NAME));
                consumerCounter.inc();

                final String kafkaQuotaClientId = getKafkaQuotaClientId(eventTypeName, client);

                response.setStatus(HttpStatus.OK.value());
                response.setContentType("application/x-json-stream");
                final EventConsumer eventConsumer = timelineService.createEventConsumer(
                        kafkaQuotaClientId, streamConfig.getCursors());

                final String bytesFlushedMetricName = MetricUtils.metricNameForLoLAStream(
                        client.getClientId(),
                        eventTypeName);

                final Meter bytesFlushedMeter = this.streamMetrics.meter(bytesFlushedMetricName);

                eventStream = eventStreamFactory.createEventStream(
                        outputStream, eventConsumer, streamConfig, blacklistService, cursorConverter,
                        bytesFlushedMeter);

                outputStream.flush(); // Flush status code to client

                eventStream.streamEvents(connectionReady);
            } catch (final UnparseableCursorException e) {
                LOG.debug("Incorrect syntax of X-nakadi-cursors header: {}. Respond with BAD_REQUEST.",
                        e.getCursors(), e);
                writeProblemResponse(response, outputStream, BAD_REQUEST, e.getMessage());
            } catch (final NoSuchEventTypeException e) {
                writeProblemResponse(response, outputStream, NOT_FOUND, "topic not found");
            } catch (final NoConnectionSlotsException e) {
                LOG.debug("Connection creation failed due to exceeding max connection count");
                writeProblemResponse(response, outputStream, e.asProblem());
            } catch (final NakadiException e) {
                LOG.error("Error while trying to stream events.", e);
                writeProblemResponse(response, outputStream, e.asProblem());
            } catch (final InvalidCursorException e) {
                writeProblemResponse(response, outputStream, PRECONDITION_FAILED, e.getMessage());
            } catch (final IllegalScopeException e) {
                writeProblemResponse(response, outputStream, FORBIDDEN, e.getMessage());
            } catch (final Exception e) {
                LOG.error("Error while trying to stream events. Respond with INTERNAL_SERVER_ERROR.", e);
                writeProblemResponse(response, outputStream, INTERNAL_SERVER_ERROR, e.getMessage());
            } finally {
                connectionReady.set(false);
                consumerLimitingService.releaseConnectionSlots(connectionSlots);
                if (consumerCounter != null) {
                    consumerCounter.dec();
                }
                if (eventStream != null) {
                    eventStream.close();
                }
                try {
                    outputStream.flush();
                } finally {
                    outputStream.close();
                }
            }
        };
    }

    /**
     * Every consumer identifies itself using a client-id to use its quota. The client id is a combination of
     * application name and event type so that every application can consume up to the quota limit per partition, given
     * partitions from the same event type are located in different brokers. In case of broker failure, multiple
     * partitions from the same event type could be served by the same broker due to Kafka fallback. In this case, the
     * quota would be shared between partitions, reducing the overall throughput for that event type.
     **/
    private String getKafkaQuotaClientId(final String eventTypeName, final Client client) {
        return client.getClientId() + "-" + eventTypeName;
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
