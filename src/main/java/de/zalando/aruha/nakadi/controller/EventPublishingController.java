package de.zalando.aruha.nakadi.controller;

import com.codahale.metrics.annotation.Timed;
import de.zalando.aruha.nakadi.domain.EventPublishResult;
import de.zalando.aruha.nakadi.exceptions.NakadiException;
import de.zalando.aruha.nakadi.exceptions.NoSuchEventTypeException;
import de.zalando.aruha.nakadi.metrics.EventTypeMetricRegistry;
import de.zalando.aruha.nakadi.metrics.EventTypeMetrics;
import de.zalando.aruha.nakadi.service.EventPublisher;
import javax.ws.rs.core.Response;
import org.json.JSONArray;
import org.json.JSONException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import static org.springframework.http.ResponseEntity.status;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import static org.springframework.web.bind.annotation.RequestMethod.POST;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.context.request.NativeWebRequest;
import org.zalando.problem.Problem;
import static org.zalando.problem.spring.web.advice.Responses.create;

@RestController
public class EventPublishingController {

    private static final Logger LOG = LoggerFactory.getLogger(EventPublishingController.class);

    private final EventPublisher publisher;
    private final EventTypeMetricRegistry eventTypeMetricRegistry;

    public EventPublishingController(final EventPublisher publisher, final EventTypeMetricRegistry eventTypeMetricRegistry) {
        this.publisher = publisher;
        this.eventTypeMetricRegistry = eventTypeMetricRegistry;
    }

    @Timed(name = "post_events", absolute = true)
    @RequestMapping(value = "/event-types/{eventTypeName}/events", method = POST)
    public ResponseEntity postEvent(@PathVariable final String eventTypeName, @RequestBody final String eventsAsString,
                                    final NativeWebRequest nativeWebRequest) {
        LOG.trace("Received event {} for event type {}", eventsAsString, eventTypeName);
        final EventTypeMetrics eventTypeMetrics = eventTypeMetricRegistry.metricsFor(eventTypeName);

        try {
            final ResponseEntity response = postEventInternal(eventTypeName, eventsAsString, nativeWebRequest, eventTypeMetrics);
            eventTypeMetrics.incrementResponseCount(response.getStatusCode().value());
            return response;
        } catch (RuntimeException ex) {
            eventTypeMetrics.incrementResponseCount(Response.Status.INTERNAL_SERVER_ERROR.getStatusCode());
            throw ex;
        }
    }

    private ResponseEntity postEventInternal(String eventTypeName, String eventsAsString, NativeWebRequest nativeWebRequest, EventTypeMetrics eventTypeMetrics) {
        final long startingNanos = System.nanoTime();
        try {
            final JSONArray eventsAsJsonObjects = new JSONArray(eventsAsString);

            final int eventCount = eventsAsJsonObjects.length();
            eventTypeMetrics.reportSizing(eventCount, eventsAsString.length());

            return response(publisher.publish(eventsAsJsonObjects, eventTypeName));
        } catch (final JSONException e) {
            LOG.debug("Problem parsing event", e);
            return create(Problem.valueOf(Response.Status.BAD_REQUEST), nativeWebRequest);
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
