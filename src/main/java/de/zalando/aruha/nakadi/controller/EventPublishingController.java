package de.zalando.aruha.nakadi.controller;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.codahale.metrics.annotation.Timed;
import de.zalando.aruha.nakadi.domain.EventPublishResult;
import de.zalando.aruha.nakadi.exceptions.NakadiException;
import de.zalando.aruha.nakadi.exceptions.NoSuchEventTypeException;
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

import static de.zalando.aruha.nakadi.metrics.MetricUtils.metricNameFor;
import static org.springframework.http.ResponseEntity.status;
import static org.springframework.web.bind.annotation.RequestMethod.POST;
import static org.zalando.problem.spring.web.advice.Responses.create;

@RestController
public class EventPublishingController {

    private static final Logger LOG = LoggerFactory.getLogger(EventPublishingController.class);
    public static final String SUCCESS_METRIC_NAME = "success";
    public static final String FAILED_METRIC_NAME = "failed";

    private final MetricRegistry metricRegistry;
    private final EventPublisher publisher;

    public EventPublishingController(final EventPublisher publisher, final MetricRegistry metricRegistry) {
        this.publisher = publisher;
        this.metricRegistry = metricRegistry;
    }

    @Timed(name = "post_events", absolute = true)
    @RequestMapping(value = "/event-types/{eventTypeName}/events", method = POST)
    public ResponseEntity postEvent(@PathVariable final String eventTypeName, @RequestBody final String event,
                                    final NativeWebRequest nativeWebRequest) {
        LOG.trace("Received event {} for event type {}", event, eventTypeName);

        try {
            return doWithMetrics(eventTypeName, System.nanoTime(), () -> {
                final JSONArray events = new JSONArray(event);
                final EventPublishResult result = publisher.publish(events, eventTypeName);
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

    private ResponseEntity response(final EventPublishResult result) {
        switch (result.getStatus()) {
            case SUBMITTED: return status(HttpStatus.OK).build();
            case ABORTED: return status(HttpStatus.UNPROCESSABLE_ENTITY).body(result.getResponses());
            default: return status(HttpStatus.MULTI_STATUS).body(result.getResponses());
        }
    }

    private ResponseEntity doWithMetrics(final String eventTypeName, final long startingNanos,
                                         final EventProcessingTask task) throws NakadiException {
        try {
            final ResponseEntity responseEntity = task.execute();

            final Timer successfullyPublishedTimer = metricRegistry.timer(metricNameFor(eventTypeName, SUCCESS_METRIC_NAME));
            successfullyPublishedTimer.update(System.nanoTime() - startingNanos, TimeUnit.NANOSECONDS);

            return responseEntity;
        } catch (final Exception e) {
            metricRegistry.counter(metricNameFor(eventTypeName, FAILED_METRIC_NAME)).inc();
            throw e;
        }
    }

    private interface EventProcessingTask {
        ResponseEntity execute() throws NakadiException;
    }
}
