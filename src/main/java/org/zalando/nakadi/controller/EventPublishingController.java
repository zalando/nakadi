package org.zalando.nakadi.controller;

import com.google.common.base.Charsets;
import io.opentracing.Span;
import io.opentracing.tag.Tags;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.zalando.nakadi.domain.EventPublishResult;
import org.zalando.nakadi.domain.EventPublishingStatus;
import org.zalando.nakadi.exceptions.runtime.AccessDeniedException;
import org.zalando.nakadi.exceptions.runtime.BlockedException;
import org.zalando.nakadi.exceptions.runtime.EventTypeTimeoutException;
import org.zalando.nakadi.exceptions.runtime.InternalNakadiException;
import org.zalando.nakadi.exceptions.runtime.NoSuchEventTypeException;
import org.zalando.nakadi.exceptions.runtime.ServiceTemporarilyUnavailableException;
import org.zalando.nakadi.metrics.EventTypeMetricRegistry;
import org.zalando.nakadi.metrics.EventTypeMetrics;
import org.zalando.nakadi.security.Client;
import org.zalando.nakadi.service.BlacklistService;
import org.zalando.nakadi.service.EventPublisher;
import org.zalando.nakadi.service.NakadiKpiPublisher;
import org.zalando.nakadi.service.TracingService;

import javax.servlet.http.HttpServletRequest;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.springframework.http.ResponseEntity.status;
import static org.springframework.web.bind.annotation.RequestMethod.POST;
import static org.zalando.problem.Status.INTERNAL_SERVER_ERROR;
import static org.zalando.problem.Status.NOT_FOUND;

@RestController
public class EventPublishingController {

    private static final Logger LOG = LoggerFactory.getLogger(EventPublishingController.class);

    private final EventPublisher publisher;
    private final EventTypeMetricRegistry eventTypeMetricRegistry;
    private final BlacklistService blacklistService;
    private final NakadiKpiPublisher nakadiKpiPublisher;
    private final String kpiBatchPublishedEventType;

    @Autowired
    public EventPublishingController(final EventPublisher publisher,
                                     final EventTypeMetricRegistry eventTypeMetricRegistry,
                                     final BlacklistService blacklistService,
                                     final NakadiKpiPublisher nakadiKpiPublisher,
                                     @Value("${nakadi.kpi.event-types.nakadiBatchPublished}") final
                                     String kpiBatchPublishedEventType) {
        this.publisher = publisher;
        this.eventTypeMetricRegistry = eventTypeMetricRegistry;
        this.blacklistService = blacklistService;
        this.nakadiKpiPublisher = nakadiKpiPublisher;
        this.kpiBatchPublishedEventType = kpiBatchPublishedEventType;
    }

    @RequestMapping(value = "/event-types/{eventTypeName}/events", method = POST)
    public ResponseEntity postEvent(@PathVariable final String eventTypeName,
                                    @RequestBody final String eventsAsString,
                                    final HttpServletRequest request,
                                    final Client client)
            throws AccessDeniedException, BlockedException, ServiceTemporarilyUnavailableException,
            InternalNakadiException, EventTypeTimeoutException, NoSuchEventTypeException {
        LOG.trace("Received event {} for event type {}", eventsAsString, eventTypeName);
        final EventTypeMetrics eventTypeMetrics = eventTypeMetricRegistry.metricsFor(eventTypeName);
        if (blacklistService.isProductionBlocked(eventTypeName, client.getClientId())) {
            throw new BlockedException("Application or event type is blocked");
        }
        try {
            final ResponseEntity response = postEventInternal(
                    eventTypeName, eventsAsString, eventTypeMetrics, client, request);
            eventTypeMetrics.incrementResponseCount(response.getStatusCode().value());
            return response;
        } catch (final NoSuchEventTypeException exception) {
            eventTypeMetrics.incrementResponseCount(NOT_FOUND.getStatusCode());
            throw exception;
        } catch (final RuntimeException ex) {
            eventTypeMetrics.incrementResponseCount(INTERNAL_SERVER_ERROR.getStatusCode());
            throw ex;
        }
    }

    private ResponseEntity postEventInternal(final String eventTypeName,
                                             final String eventsAsString,
                                             final EventTypeMetrics eventTypeMetrics,
                                             final Client client,
                                             final HttpServletRequest request)
            throws AccessDeniedException, ServiceTemporarilyUnavailableException, InternalNakadiException,
            EventTypeTimeoutException, NoSuchEventTypeException {
        final long startingNanos = System.nanoTime();
        try {
            final int totalSizeBytes = eventsAsString.getBytes(Charsets.UTF_8).length;
            final Span publishingSpan = TracingService.extractSpan(request, "publish_events")
                    .setTag("event_type", eventTypeName)
                    .setTag("slo_bucket", TracingService.getSLOBucket(totalSizeBytes))
                    .setTag(Tags.SPAN_KIND_PRODUCER, client.getClientId());

            final EventPublishResult result = publisher.publish(eventsAsString, eventTypeName, publishingSpan);

            final int eventCount = result.getResponses().size();

            reportMetrics(eventTypeMetrics, result, totalSizeBytes, eventCount);
            reportSLOs(startingNanos, totalSizeBytes, eventCount, result, eventTypeName, client);

            return response(result);
        } finally {
            eventTypeMetrics.updateTiming(startingNanos, System.nanoTime());
        }
    }

    private void reportSLOs(final long startingNanos, final int totalSizeBytes, final int eventCount,
                            final EventPublishResult eventPublishResult, final String eventTypeName,
                            final Client client) {
        if (eventPublishResult.getStatus() == EventPublishingStatus.SUBMITTED) {
            final long msSpent = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startingNanos);
            final String applicationName = client.getClientId();

            nakadiKpiPublisher.publish(kpiBatchPublishedEventType, () -> new JSONObject()
                    .put("event_type", eventTypeName)
                    .put("app", applicationName)
                    .put("app_hashed", nakadiKpiPublisher.hash(applicationName))
                    .put("token_realm", client.getRealm())
                    .put("number_of_events", eventCount)
                    .put("ms_spent", msSpent)
                    .put("batch_size", totalSizeBytes));
        }
    }

    private void reportMetrics(final EventTypeMetrics eventTypeMetrics, final EventPublishResult result,
                               final int totalSizeBytes, final int eventCount) {
        if (result.getStatus() == EventPublishingStatus.SUBMITTED) {
            eventTypeMetrics.reportSizing(eventCount, totalSizeBytes - eventCount - 1);
        } else if (result.getStatus() == EventPublishingStatus.FAILED && eventCount != 0) {
            final int successfulEvents = result.getResponses()
                    .stream()
                    .filter(r -> r.getPublishingStatus() == EventPublishingStatus.SUBMITTED)
                    .collect(Collectors.toList())
                    .size();
            final double avgEventSize = totalSizeBytes / (double) eventCount;
            eventTypeMetrics.reportSizing(successfulEvents, (int) Math.round(avgEventSize * successfulEvents));
        }
    }

    private ResponseEntity response(final EventPublishResult result) {
        switch (result.getStatus()) {
            case SUBMITTED:
                return status(HttpStatus.OK).build();
            case ABORTED:
                return status(HttpStatus.UNPROCESSABLE_ENTITY).body(result.getResponses());
            default:
                return status(HttpStatus.MULTI_STATUS).body(result.getResponses());
        }
    }
}
