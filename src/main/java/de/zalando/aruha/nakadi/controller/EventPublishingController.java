package de.zalando.aruha.nakadi.controller;

import com.codahale.metrics.annotation.Timed;
import de.zalando.aruha.nakadi.domain.EventPublishResult;
import de.zalando.aruha.nakadi.exceptions.NakadiException;
import de.zalando.aruha.nakadi.exceptions.NoSuchEventTypeException;
import de.zalando.aruha.nakadi.metrics.EventTypeMetricRegistry;
import de.zalando.aruha.nakadi.metrics.EventTypeMetrics;
import de.zalando.aruha.nakadi.service.EventPublisher;
import org.json.JSONArray;
import org.json.JSONException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.context.request.NativeWebRequest;
import org.zalando.problem.Problem;

import javax.ws.rs.core.Response;
import java.util.concurrent.TimeUnit;

import static org.springframework.http.ResponseEntity.status;
import static org.springframework.web.bind.annotation.RequestMethod.POST;
import static org.zalando.problem.spring.web.advice.Responses.create;

@RestController
public class EventPublishingController {

    private static final Logger LOG = LoggerFactory.getLogger(EventPublishingController.class);
    public static final String SUCCESS_METRIC_NAME = "success";
    public static final String FAILED_METRIC_NAME = "failed";

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

        try {
            final EventTypeMetrics eventTypeMetrics = eventTypeMetricRegistry.metricsFor(eventTypeName);

            return doWithTimerMetric(eventTypeMetrics, () -> {
                final JSONArray eventsAsJsonObjects = new JSONArray(eventsAsString);

                reportMetrics(eventTypeMetrics, eventsAsString, eventsAsJsonObjects);

                final EventPublishResult result = publisher.publish(eventsAsJsonObjects, eventTypeName);
                return response(result);
            });
        } catch (final JSONException e) {
            LOG.debug("Problem parsing event", e);
            return create(Problem.valueOf(Response.Status.BAD_REQUEST), nativeWebRequest);
        } catch (final NoSuchEventTypeException e) {
            LOG.debug("Event type not found.", e);
            return create(e.asProblem(), nativeWebRequest);
        } catch (final NakadiException e) {
            LOG.debug("Failed to publish batch", e);
            return create(e.asProblem(), nativeWebRequest);
        }
    }

    private void reportMetrics(final EventTypeMetrics eventTypeMetrics, final String eventsAsString, final JSONArray eventsAsJsonObjects) {
        final int numberOfEventsInBatch = eventsAsJsonObjects.length();
        eventTypeMetrics.getEventsPerBatchHistogram().update(numberOfEventsInBatch);
        eventTypeMetrics.getAverageEventSizeInBytesHistogram().update(eventsAsString.length() / numberOfEventsInBatch);
    }

    private ResponseEntity response(final EventPublishResult result) {
        switch (result.getStatus()) {
            case SUBMITTED: return status(HttpStatus.OK).build();
            case ABORTED: return status(HttpStatus.UNPROCESSABLE_ENTITY).body(result.getResponses());
            default: return status(HttpStatus.MULTI_STATUS).body(result.getResponses());
        }
    }

    private ResponseEntity doWithTimerMetric(final EventTypeMetrics eventTypeMetrics,
                                             final EventProcessingTask task) throws NakadiException {
        try {
            final long startingNanos = System.nanoTime();

            final ResponseEntity responseEntity = task.execute();

            eventTypeMetrics.getSuccessfullyPublishedTimer().update(System.nanoTime() - startingNanos, TimeUnit.NANOSECONDS);

            return responseEntity;
        } catch (final Exception e) {
            eventTypeMetrics.getFailedPublishedCounter().inc();
            throw e;
        }
    }

    private interface EventProcessingTask {
        ResponseEntity execute() throws NakadiException;
    }
}
