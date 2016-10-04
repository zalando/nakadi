package org.zalando.nakadi.controller;

import org.json.JSONArray;
import org.json.JSONException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.context.request.NativeWebRequest;
import org.zalando.nakadi.domain.EventPublishResult;
import org.zalando.nakadi.exceptions.NakadiException;
import org.zalando.nakadi.exceptions.NoSuchEventTypeException;
import org.zalando.nakadi.metrics.EventTypeMetricRegistry;
import org.zalando.nakadi.metrics.EventTypeMetrics;
import org.zalando.nakadi.throttling.ThrottleResult;
import org.zalando.nakadi.throttling.ThrottlingService;
import org.zalando.nakadi.security.Client;
import org.zalando.nakadi.service.EventPublisher;
import org.zalando.problem.Problem;
import org.zalando.problem.ThrowableProblem;

import javax.ws.rs.core.Response;

import static org.springframework.http.ResponseEntity.status;
import static org.springframework.web.bind.annotation.RequestMethod.POST;
import static org.zalando.problem.spring.web.advice.Responses.create;

@RestController
public class EventPublishingController {

    private static final Logger LOG = LoggerFactory.getLogger(EventPublishingController.class);

    private final EventPublisher publisher;
    private final EventTypeMetricRegistry eventTypeMetricRegistry;
    private final ThrottlingService throttlingService;

    @Autowired
    public EventPublishingController(final EventPublisher publisher,
                                     final EventTypeMetricRegistry eventTypeMetricRegistry,
                                     final ThrottlingService throttlingService) {
        this.publisher = publisher;
        this.eventTypeMetricRegistry = eventTypeMetricRegistry;
        this.throttlingService = throttlingService;
    }

    @RequestMapping(value = "/event-types/{eventTypeName}/events", method = POST)
    public ResponseEntity<?> postEvent(@PathVariable final String eventTypeName,
                                    @RequestBody final String eventsAsString,
                                    final NativeWebRequest nativeWebRequest,
                                    final Client client) {
        LOG.trace("Received event {} for event type {}", eventsAsString, eventTypeName);
        final EventTypeMetrics eventTypeMetrics = eventTypeMetricRegistry.metricsFor(eventTypeName);

        try {
            final ResponseEntity response = postEventInternal(eventTypeName, eventsAsString,
                    nativeWebRequest, eventTypeMetrics, client);
            eventTypeMetrics.incrementResponseCount(response.getStatusCode().value());
            return response;
        } catch (RuntimeException ex) {
            eventTypeMetrics.incrementResponseCount(Response.Status.INTERNAL_SERVER_ERROR.getStatusCode());
            throw ex;
        }
    }

    private ResponseEntity<?> postEventInternal(final String eventTypeName,
                                             final String eventsAsString,
                                             final NativeWebRequest nativeWebRequest,
                                             final EventTypeMetrics eventTypeMetrics,
                                             final Client client) {
        final long startingNanos = System.nanoTime();
        try {
            final JSONArray eventsAsJsonObjects = new JSONArray(eventsAsString);

            final int eventCount = eventsAsJsonObjects.length();
            final int size = eventsAsString.getBytes().length;
            eventTypeMetrics.reportSizing(eventCount, size);
            ThrottleResult result = throttlingService.mark(client.getId(), eventTypeName, size, eventCount);
            return result.isThrottled()
                    ? rateLimitHeaders(ResponseEntity.status(429), result).build()
                    : response(publisher.publish(eventsAsJsonObjects, eventTypeName, client), result);

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

    private ResponseEntity<?> processJSONException(final JSONException e, final NativeWebRequest nativeWebRequest) {
        if (e.getCause() == null) {
            return create(createProblem(e), nativeWebRequest);
        }
        return create(Problem.valueOf(Response.Status.BAD_REQUEST), nativeWebRequest);
    }

    private ThrowableProblem createProblem(final JSONException e) {
        return Problem.valueOf(Response.Status.BAD_REQUEST, e.getMessage());
    }

    private ResponseEntity<?> response(final EventPublishResult result, final ThrottleResult throttleResult) {
        switch (result.getStatus()) {
            case SUBMITTED:
                return rateLimitHeaders(status(HttpStatus.OK), throttleResult).build();
            case ABORTED:
                return rateLimitHeaders(status(HttpStatus.UNPROCESSABLE_ENTITY), throttleResult)
                        .body(result.getResponses());
            default:
                return rateLimitHeaders(status(HttpStatus.MULTI_STATUS), throttleResult).body(result.getResponses());
        }
    }

    private ResponseEntity.BodyBuilder rateLimitHeaders(ResponseEntity.BodyBuilder builder, ThrottleResult result) {
        return builder
                .header("X-Rate-Bytes-Limit", Long.toString(result.getBytesLimit()))
                .header("X-Rate-Bytes-Remaining", Long.toString(result.getBytesRemaining()))
                .header("X-Rate-Batches-Limit", Long.toString(result.getBatchesLimit()))
                .header("X-Rate-Batches-Remaining", Long.toString(result.getBatchesRemaining()))
                .header("X-Rate-Messages-Limit", Long.toString(result.getMessagesLimit()))
                .header("X-Rate-Messages-Remaining", Long.toString(result.getMessagesRemaining()))
                .header("X-Rate-Reset", Long.toString(result.getReset().getMillis()));
    }
}
