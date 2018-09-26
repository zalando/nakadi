package org.zalando.nakadi.controller;

import com.google.common.base.Charsets;
import org.json.JSONException;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.context.request.NativeWebRequest;
import org.zalando.nakadi.domain.EventPublishResult;
import org.zalando.nakadi.domain.EventPublishingStatus;
import org.zalando.nakadi.exceptions.runtime.AccessDeniedException;
import org.zalando.nakadi.exceptions.runtime.EnrichmentException;
import org.zalando.nakadi.exceptions.runtime.EventTypeTimeoutException;
import org.zalando.nakadi.exceptions.runtime.InternalNakadiException;
import org.zalando.nakadi.exceptions.runtime.InvalidPartitionKeyFieldsException;
import org.zalando.nakadi.exceptions.runtime.NoSuchEventTypeException;
import org.zalando.nakadi.exceptions.runtime.PartitioningException;
import org.zalando.nakadi.exceptions.runtime.ServiceTemporarilyUnavailableException;
import org.zalando.nakadi.metrics.EventTypeMetricRegistry;
import org.zalando.nakadi.metrics.EventTypeMetrics;
import org.zalando.nakadi.security.Client;
import org.zalando.nakadi.service.BlacklistService;
import org.zalando.nakadi.service.EventPublisher;
import org.zalando.nakadi.service.NakadiKpiPublisher;
import org.zalando.problem.Problem;
import org.zalando.problem.ThrowableProblem;
import org.zalando.problem.spring.web.advice.Responses;

import javax.ws.rs.core.Response;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static javax.ws.rs.core.Response.Status.INTERNAL_SERVER_ERROR;
import static javax.ws.rs.core.Response.Status.NOT_FOUND;
import static javax.ws.rs.core.Response.Status.SERVICE_UNAVAILABLE;
import static org.springframework.http.ResponseEntity.status;
import static org.springframework.web.bind.annotation.RequestMethod.POST;
import static org.zalando.problem.MoreStatus.UNPROCESSABLE_ENTITY;
import static org.zalando.problem.spring.web.advice.Responses.create;

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
                                     @Value("${nakadi.kpi.event-types.nakadiBatchPublished}")
                                         final String kpiBatchPublishedEventType) {
        this.publisher = publisher;
        this.eventTypeMetricRegistry = eventTypeMetricRegistry;
        this.blacklistService = blacklistService;
        this.nakadiKpiPublisher = nakadiKpiPublisher;
        this.kpiBatchPublishedEventType = kpiBatchPublishedEventType;
    }

    @RequestMapping(value = "/event-types/{eventTypeName}/events", method = POST)
    public ResponseEntity postEvent(@PathVariable final String eventTypeName,
                                    @RequestBody final String eventsAsString,
                                    final NativeWebRequest request,
                                    final Client client)
            throws AccessDeniedException, EnrichmentException,
            PartitioningException, ServiceTemporarilyUnavailableException {
        LOG.trace("Received event {} for event type {}", eventsAsString, eventTypeName);
        final EventTypeMetrics eventTypeMetrics = eventTypeMetricRegistry.metricsFor(eventTypeName);

        try {
            if (blacklistService.isProductionBlocked(eventTypeName, client.getClientId())) {
                return Responses.create(
                        Problem.valueOf(Response.Status.FORBIDDEN, "Application or event type is blocked"), request);
            }

            final ResponseEntity response = postEventInternal(
                    eventTypeName, eventsAsString, request, eventTypeMetrics, client);
            eventTypeMetrics.incrementResponseCount(response.getStatusCode().value());
            return response;
        } catch (final RuntimeException ex) {
            eventTypeMetrics.incrementResponseCount(Response.Status.INTERNAL_SERVER_ERROR.getStatusCode());
            throw ex;
        }
    }

    @ExceptionHandler(EnrichmentException.class)
    public ResponseEntity<Problem> handleEnrichmentException(final EnrichmentException exception,
                                                             final NativeWebRequest request) {
        LOG.debug(exception.getMessage());
        return Responses.create(UNPROCESSABLE_ENTITY, exception.getMessage(), request);
    }

    @ExceptionHandler(PartitioningException.class)
    public ResponseEntity<Problem> handlePartitioningException(final PartitioningException exception,
                                                               final NativeWebRequest request) {
        LOG.debug(exception.getMessage());
        return Responses.create(UNPROCESSABLE_ENTITY, exception.getMessage(), request);
    }

    @ExceptionHandler(InvalidPartitionKeyFieldsException.class)
    public ResponseEntity<Problem> handleInvalidPartitionKeyFieldsException(
            final InvalidPartitionKeyFieldsException exception,
            final NativeWebRequest request) {
        LOG.debug(exception.getMessage());
        return Responses.create(UNPROCESSABLE_ENTITY, exception.getMessage(), request);
    }

    private ResponseEntity postEventInternal(final String eventTypeName,
                                             final String eventsAsString,
                                             final NativeWebRequest nativeWebRequest,
                                             final EventTypeMetrics eventTypeMetrics,
                                             final Client client)
            throws AccessDeniedException, EnrichmentException, PartitioningException,
            ServiceTemporarilyUnavailableException {
        final long startingNanos = System.nanoTime();
        try {
            final EventPublishResult result = publisher.publish(eventsAsString, eventTypeName);

            final int eventCount = result.getResponses().size();
            final int totalSizeBytes = eventsAsString.getBytes(Charsets.UTF_8).length;

            reportMetrics(eventTypeMetrics, result, totalSizeBytes, eventCount);
            reportSLOs(startingNanos, totalSizeBytes, eventCount, result, eventTypeName, client);

            return response(result);
        } catch (final JSONException e) {
            LOG.debug("Problem parsing event", e);
            return processJSONException(e, nativeWebRequest);
        } catch (final NoSuchEventTypeException e) {
            LOG.debug("Event type not found.", e.getMessage());
            return create(Problem.valueOf(NOT_FOUND, e.getMessage()), nativeWebRequest);
        } catch (final EventTypeTimeoutException e) {
            LOG.debug("Failed to publish batch", e);
            return create(Problem.valueOf(SERVICE_UNAVAILABLE, e.getMessage()), nativeWebRequest);
        } catch (final InternalNakadiException e) {
            LOG.debug("Failed to publish batch", e);
            return create(Problem.valueOf(INTERNAL_SERVER_ERROR, e.getMessage()), nativeWebRequest);
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

    private ResponseEntity processJSONException(final JSONException e, final NativeWebRequest nativeWebRequest) {
        if (e.getCause() == null) {
            return create(createProblem(e), nativeWebRequest);
        }
        return create(Problem.valueOf(Response.Status.BAD_REQUEST), nativeWebRequest);
    }

    private ThrowableProblem createProblem(final JSONException e) {
        return Problem.valueOf(Response.Status.BAD_REQUEST, "Error occurred when parsing event(s). " + e.getMessage());
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
