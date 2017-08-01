package org.zalando.nakadi.controller;

import com.google.common.base.Charsets;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import javax.ws.rs.core.Response;
import org.json.JSONException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import static org.springframework.http.ResponseEntity.status;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import static org.springframework.web.bind.annotation.RequestMethod.POST;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.context.request.NativeWebRequest;
import org.zalando.nakadi.domain.EventPublishResult;
import org.zalando.nakadi.domain.EventPublishingStatus;
import org.zalando.nakadi.exceptions.NakadiException;
import org.zalando.nakadi.exceptions.NoSuchEventTypeException;
import org.zalando.nakadi.exceptions.runtime.AccessDeniedException;
import org.zalando.nakadi.exceptions.runtime.ServiceTemporarilyUnavailableException;
import org.zalando.nakadi.metrics.EventTypeMetricRegistry;
import org.zalando.nakadi.metrics.EventTypeMetrics;
import org.zalando.nakadi.security.Client;
import org.zalando.nakadi.service.BlacklistService;
import org.zalando.nakadi.service.EventPublisher;
import org.zalando.problem.Problem;
import org.zalando.problem.ThrowableProblem;
import org.zalando.problem.spring.web.advice.Responses;
import static org.zalando.problem.spring.web.advice.Responses.create;

@RestController
public class EventPublishingController {

    private static final Logger LOG = LoggerFactory.getLogger(EventPublishingController.class);

    private final EventPublisher publisher;
    private final EventTypeMetricRegistry eventTypeMetricRegistry;
    private final BlacklistService blacklistService;

    @Autowired
    public EventPublishingController(final EventPublisher publisher,
                                     final EventTypeMetricRegistry eventTypeMetricRegistry,
                                     final BlacklistService blacklistService) {
        this.publisher = publisher;
        this.eventTypeMetricRegistry = eventTypeMetricRegistry;
        this.blacklistService = blacklistService;
    }

    @RequestMapping(value = "/event-types/{eventTypeName}/events", method = POST)
    public ResponseEntity postEvent(@PathVariable final String eventTypeName,
                                    @RequestBody final String eventsAsString,
                                    final NativeWebRequest request,
                                    final Client client) throws AccessDeniedException {
        LOG.trace("Received event {} for event type {}", eventsAsString, eventTypeName);
        final EventTypeMetrics eventTypeMetrics = eventTypeMetricRegistry.metricsFor(eventTypeName);

        try {
            if (blacklistService.isProductionBlocked(eventTypeName, client.getClientId())) {
                return Responses.create(
                        Problem.valueOf(Response.Status.FORBIDDEN, "Application or event type is blocked"), request);
            }

            final ResponseEntity response = postEventInternal(eventTypeName, eventsAsString,
                    request, eventTypeMetrics, client);
            eventTypeMetrics.incrementResponseCount(response.getStatusCode().value());
            return response;
        } catch (RuntimeException ex) {
            eventTypeMetrics.incrementResponseCount(Response.Status.INTERNAL_SERVER_ERROR.getStatusCode());
            throw ex;
        }
    }

    private ResponseEntity postEventInternal(final String eventTypeName,
                                             final String eventsAsString,
                                             final NativeWebRequest nativeWebRequest,
                                             final EventTypeMetrics eventTypeMetrics,
                                             final Client client)
            throws AccessDeniedException, ServiceTemporarilyUnavailableException {
        final long startingNanos = System.nanoTime();
        try {
            final EventPublishResult result = publisher.publish(eventsAsString, eventTypeName, client);

            final int eventCount = result.getResponses().size();
            final int totalSizeBytes = eventsAsString.getBytes(Charsets.UTF_8).length;

            reportMetrics(eventTypeMetrics, result, totalSizeBytes, eventCount);
            reportSLOs(startingNanos, totalSizeBytes, eventCount, result);

            return response(result);
        } catch (final JSONException e) {
            LOG.debug("Problem parsing event", e);
            return processJSONException(e, nativeWebRequest);
        } catch (final NoSuchEventTypeException e) {
            LOG.debug("Event type not found.", e);
            return create(e.asProblem(), nativeWebRequest);
        } catch (final NakadiException e) {
            LOG.debug("Failed to publish batch", e);
            return create(e.asProblem(), nativeWebRequest);
        } finally {
            eventTypeMetrics.updateTiming(startingNanos, System.nanoTime());
        }
    }

    private void reportSLOs(final long startingNanos, final int totalSizeBytes, final int eventCount,
                            final EventPublishResult eventPublishResult) {
        if (eventPublishResult.getStatus() == EventPublishingStatus.SUBMITTED) {
            final long msSpent = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startingNanos);
            LOG.info("[SLO] [publishing-latency] time={} size={} count={}", msSpent, totalSizeBytes, eventCount);
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
        return Problem.valueOf(Response.Status.BAD_REQUEST, e.getMessage());
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
