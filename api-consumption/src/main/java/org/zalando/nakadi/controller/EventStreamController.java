package org.zalando.nakadi.controller;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.servlet.mvc.method.annotation.StreamingResponseBody;
import org.zalando.nakadi.cache.EventTypeCache;
import org.zalando.nakadi.domain.CursorError;
import org.zalando.nakadi.domain.EventType;
import org.zalando.nakadi.domain.NakadiCursor;
import org.zalando.nakadi.domain.PartitionStatistics;
import org.zalando.nakadi.domain.Timeline;
import org.zalando.nakadi.domain.storage.Storage;
import org.zalando.nakadi.exceptions.runtime.AccessDeniedException;
import org.zalando.nakadi.exceptions.runtime.InternalNakadiException;
import org.zalando.nakadi.exceptions.runtime.InvalidCursorException;
import org.zalando.nakadi.exceptions.runtime.InvalidLimitException;
import org.zalando.nakadi.exceptions.runtime.NoConnectionSlotsException;
import org.zalando.nakadi.exceptions.runtime.NoSuchEventTypeException;
import org.zalando.nakadi.exceptions.runtime.ServiceTemporarilyUnavailableException;
import org.zalando.nakadi.exceptions.runtime.UnparseableCursorException;
import org.zalando.nakadi.metrics.MetricUtils;
import org.zalando.nakadi.repository.EventConsumer;
import org.zalando.nakadi.repository.TopicRepository;
import org.zalando.nakadi.security.Client;
import org.zalando.nakadi.service.AuthorizationValidator;
import org.zalando.nakadi.service.CursorConverter;
import org.zalando.nakadi.service.EventStream;
import org.zalando.nakadi.service.EventStreamChecks;
import org.zalando.nakadi.service.EventStreamConfig;
import org.zalando.nakadi.service.EventStreamFactory;
import org.zalando.nakadi.service.EventTypeChangeListener;
import org.zalando.nakadi.service.timeline.TimelineService;
import org.zalando.nakadi.util.MDCUtils;
import org.zalando.nakadi.view.Cursor;
import org.zalando.problem.Problem;
import org.zalando.problem.StatusType;

import javax.annotation.Nullable;
import javax.servlet.http.HttpServletResponse;
import java.io.Closeable;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import static org.zalando.nakadi.metrics.MetricUtils.metricNameFor;
import static org.zalando.problem.Status.BAD_REQUEST;
import static org.zalando.problem.Status.FORBIDDEN;
import static org.zalando.problem.Status.INTERNAL_SERVER_ERROR;
import static org.zalando.problem.Status.NOT_FOUND;
import static org.zalando.problem.Status.PRECONDITION_FAILED;
import static org.zalando.problem.Status.SERVICE_UNAVAILABLE;
import static org.zalando.problem.Status.TOO_MANY_REQUESTS;
import static org.zalando.problem.Status.UNPROCESSABLE_ENTITY;

@RestController
public class EventStreamController {

    private static final Logger LOG = LoggerFactory.getLogger(EventStreamController.class);
    public static final String CONSUMERS_COUNT_METRIC_NAME = "consumers";

    private final TimelineService timelineService;
    private final ObjectMapper jsonMapper;
    private final EventStreamFactory eventStreamFactory;
    private final MetricRegistry metricRegistry;
    private final EventStreamChecks eventStreamChecks;
    private final CursorConverter cursorConverter;
    private final MetricRegistry streamMetrics;
    private final AuthorizationValidator authorizationValidator;
    private final EventTypeChangeListener eventTypeChangeListener;
    private final Long maxMemoryUsageBytes;
    private final EventTypeCache eventTypeCache;

    @Autowired
    public EventStreamController(final EventTypeCache eventTypeCache,
                                 final TimelineService timelineService,
                                 final ObjectMapper jsonMapper,
                                 final EventStreamFactory eventStreamFactory,
                                 final MetricRegistry metricRegistry,
                                 @Qualifier("streamMetricsRegistry") final MetricRegistry streamMetrics,
                                 final EventStreamChecks eventStreamChecks,
                                 final CursorConverter cursorConverter,
                                 final AuthorizationValidator authorizationValidator,
                                 final EventTypeChangeListener eventTypeChangeListener,
                                 @Value("${nakadi.stream.maxStreamMemoryBytes}") final Long maxMemoryUsageBytes) {
        this.timelineService = timelineService;
        this.jsonMapper = jsonMapper;
        this.eventStreamFactory = eventStreamFactory;
        this.metricRegistry = metricRegistry;
        this.streamMetrics = streamMetrics;
        this.eventStreamChecks = eventStreamChecks;
        this.cursorConverter = cursorConverter;
        this.authorizationValidator = authorizationValidator;
        this.eventTypeChangeListener = eventTypeChangeListener;
        this.eventTypeCache = eventTypeCache;
        this.maxMemoryUsageBytes = maxMemoryUsageBytes;
    }

    @VisibleForTesting
    List<NakadiCursor> getStreamingStart(final EventType eventType, final String cursorsStr)
            throws UnparseableCursorException, ServiceTemporarilyUnavailableException, InvalidCursorException,
            InternalNakadiException, NoSuchEventTypeException {
        List<Cursor> cursors = null;
        if (cursorsStr != null) {
            try {
                cursors = jsonMapper.readValue(cursorsStr, new TypeReference<ArrayList<Cursor>>() {
                });
            } catch (final IOException ex) {
                throw new UnparseableCursorException("incorrect syntax of X-nakadi-cursors header", ex, cursorsStr);
            }
            // Unfortunately, In order to have consistent exception checking, one can not just call validator
            for (final Cursor cursor : cursors) {
                if (null == cursor.getPartition()) {
                    throw new InvalidCursorException(CursorError.NULL_PARTITION, cursor, eventType.getName());
                } else if (null == cursor.getOffset()) {
                    throw new InvalidCursorException(CursorError.NULL_OFFSET, cursor, eventType.getName());
                }
            }
        }
        final Timeline latestTimeline = timelineService.getActiveTimeline(eventType);
        if (null != cursors) {
            if (cursors.isEmpty()) {
                throw new InvalidCursorException(CursorError.INVALID_FORMAT, eventType.getName());
            }
            final List<NakadiCursor> result = new ArrayList<>();
            for (final Cursor c : cursors) {
                result.add(cursorConverter.convert(eventType.getName(), c));
            }

            for (final NakadiCursor c : result) {
                if (c.getTimeline().isDeleted()) {
                    throw new InvalidCursorException(CursorError.UNAVAILABLE, c);
                }
            }
            final Map<Storage, List<NakadiCursor>> groupedCursors = result.stream().collect(
                    Collectors.groupingBy(c -> c.getTimeline().getStorage()));
            for (final Map.Entry<Storage, List<NakadiCursor>> entry : groupedCursors.entrySet()) {
                timelineService.getTopicRepository(entry.getKey())
                        .validateReadCursors(entry.getValue());
            }
            return result;
        } else {
            final TopicRepository latestTopicRepository = timelineService.getTopicRepository(latestTimeline);
            // if no cursors provided - read from the newest available events
            return latestTopicRepository.loadTopicStatistics(Collections.singletonList(latestTimeline))
                    .stream()
                    .map(PartitionStatistics::getLast)
                    .collect(Collectors.toList());
        }
    }

    private void authorizeStreamRead(final String eventType) throws AccessDeniedException,
            ServiceTemporarilyUnavailableException {
        try {
            authorizationValidator.authorizeStreamRead(eventTypeCache.getEventType(eventType));
        } catch (final InternalNakadiException | NoSuchEventTypeException ex) {
            throw new ServiceTemporarilyUnavailableException(ex);
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
            final HttpServletResponse response, final Client client) {
        final MDCUtils.Context requestContext = MDCUtils.getContext();

        return outputStream -> {
            try (MDCUtils.CloseableNoEx ignore1 = MDCUtils.withContext(requestContext)) {
                if (eventStreamChecks.isConsumptionBlocked(
                        Collections.singleton(eventTypeName), client.getClientId())) {
                    writeProblemResponse(response, outputStream,
                            Problem.valueOf(FORBIDDEN, "Application or event type is blocked"));
                    return;
                }

                Counter consumerCounter = null;
                EventStream eventStream = null;
                final AtomicBoolean needCheckAuthorization = new AtomicBoolean(false);

                LOG.info("[X-NAKADI-CURSORS] \"{}\" {}", eventTypeName, Optional.ofNullable(cursorsStr).orElse("-"));

                try (Closeable ignore2 = eventTypeChangeListener.registerListener(
                        et -> needCheckAuthorization.set(true),
                        Collections.singletonList(eventTypeName))) {
                    final EventType eventType = eventTypeCache.getEventType(eventTypeName);

                    authorizationValidator.authorizeEventTypeView(eventType);
                    authorizeStreamRead(eventTypeName);

                    // validate parameters
                    final EventStreamConfig streamConfig = EventStreamConfig.builder()
                            .withBatchLimit(batchLimit)
                            .withStreamLimit(streamLimit)
                            .withBatchTimeout(batchTimeout)
                            .withStreamTimeout(streamTimeout)
                            .withStreamKeepAliveLimit(streamKeepAliveLimit)
                            .withEtName(eventTypeName)
                            .withConsumingClient(client)
                            .withCursors(getStreamingStart(eventType, cursorsStr))
                            .withMaxMemoryUsageBytes(maxMemoryUsageBytes)
                            .build();

                    consumerCounter = metricRegistry.counter(metricNameFor(eventTypeName, CONSUMERS_COUNT_METRIC_NAME));
                    consumerCounter.inc();

                    final String kafkaQuotaClientId = getKafkaQuotaClientId(eventTypeName, client);

                    response.setStatus(HttpStatus.OK.value());
                    response.setHeader("Warning", "299 - nakadi - the Low-level API is deprecated and will "
                            + "be removed from a future release. Please consider migrating to the Subscriptions API.");
                    response.setContentType("application/x-json-stream");
                    final EventConsumer eventConsumer = timelineService.createEventConsumer(
                            kafkaQuotaClientId, streamConfig.getCursors());

                    final String bytesFlushedMetricName = MetricUtils.metricNameForLoLAStream(
                            client.getClientId(),
                            eventTypeName);

                    final Meter bytesFlushedMeter = this.streamMetrics.meter(bytesFlushedMetricName);

                    eventStream = eventStreamFactory.createEventStream(
                            outputStream, eventConsumer, streamConfig, bytesFlushedMeter);

                    outputStream.flush(); // Flush status code to client

                    eventStream.streamEvents(() -> {
                        if (needCheckAuthorization.getAndSet(false)) {
                            authorizeStreamRead(eventTypeName);
                        }
                    });
                } catch (final UnparseableCursorException e) {
                    LOG.debug("Incorrect syntax of X-nakadi-cursors header: {}. Respond with BAD_REQUEST.",
                            e.getCursors(), e);
                    writeProblemResponse(response, outputStream, BAD_REQUEST, e.getMessage());
                } catch (final NoSuchEventTypeException e) {
                    writeProblemResponse(response, outputStream, NOT_FOUND, "topic not found");
                } catch (final NoConnectionSlotsException e) {
                    LOG.debug("Connection creation failed due to exceeding max connection count");
                    writeProblemResponse(response, outputStream,
                            Problem.valueOf(TOO_MANY_REQUESTS, e.getMessage()));
                } catch (final ServiceTemporarilyUnavailableException e) {
                    LOG.error("Error while trying to stream events.", e);
                    writeProblemResponse(response, outputStream, SERVICE_UNAVAILABLE, e.getMessage());
                } catch (final InvalidLimitException e) {
                    writeProblemResponse(response, outputStream, UNPROCESSABLE_ENTITY, e.getMessage());
                } catch (final InternalNakadiException e) {
                    LOG.error("Error while trying to stream events.", e);
                    writeProblemResponse(response, outputStream, INTERNAL_SERVER_ERROR, e.getMessage());
                } catch (final InvalidCursorException e) {
                    writeProblemResponse(response, outputStream, PRECONDITION_FAILED, e.getMessage());
                } catch (final AccessDeniedException e) {
                    writeProblemResponse(response, outputStream, FORBIDDEN, e.explain());
                } catch (final Exception e) {
                    LOG.error("Error while trying to stream events. Respond with INTERNAL_SERVER_ERROR.", e);
                    writeProblemResponse(response, outputStream, INTERNAL_SERVER_ERROR, e.getMessage());
                } finally {
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
                                      final StatusType statusCode, final String message) throws IOException {
        writeProblemResponse(response, outputStream, Problem.valueOf(statusCode, message));
    }

    private void writeProblemResponse(final HttpServletResponse response, final OutputStream outputStream,
                                      final Problem problem) throws IOException {
        response.setStatus(problem.getStatus().getStatusCode());
        response.setContentType("application/problem+json");
        jsonMapper.writer().writeValue(outputStream, problem);
    }
}
