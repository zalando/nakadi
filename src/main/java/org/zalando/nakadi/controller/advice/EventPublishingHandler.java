package org.zalando.nakadi.controller.advice;

import org.json.JSONException;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ControllerAdvice;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.context.request.NativeWebRequest;
import org.zalando.nakadi.controller.EventPublishingController;
import org.zalando.nakadi.exceptions.runtime.EnrichmentException;
import org.zalando.nakadi.exceptions.runtime.EventTypeTimeoutException;
import org.zalando.nakadi.exceptions.runtime.InvalidPartitionKeyFieldsException;
import org.zalando.nakadi.exceptions.runtime.NakadiBaseException;
import org.zalando.nakadi.exceptions.runtime.PartitioningException;
import org.zalando.problem.Problem;
import org.zalando.problem.spring.web.advice.AdviceTrait;

import javax.annotation.Priority;

import static org.zalando.problem.Status.BAD_REQUEST;
import static org.zalando.problem.Status.SERVICE_UNAVAILABLE;
import static org.zalando.problem.Status.UNPROCESSABLE_ENTITY;

@Priority(10)
@ControllerAdvice(assignableTypes = EventPublishingController.class)
public class EventPublishingHandler implements AdviceTrait {

    @ExceptionHandler(EventTypeTimeoutException.class)
    public ResponseEntity<Problem> handleEventTypeTimeoutException(final EventTypeTimeoutException exception,
                                                                   final NativeWebRequest request) {
        return create(Problem.valueOf(SERVICE_UNAVAILABLE, exception.getMessage()), request);
    }

    @ExceptionHandler(JSONException.class)
    public ResponseEntity<Problem> handleJSONException(final JSONException exception,
                                                       final NativeWebRequest request) {
        if (exception.getCause() == null) {
            return create(Problem.valueOf(BAD_REQUEST,
                    "Error occurred when parsing event(s). " + exception.getMessage()), request);
        }
        return create(Problem.valueOf(BAD_REQUEST), request);
    }

    @ExceptionHandler({EnrichmentException.class,
            PartitioningException.class,
            InvalidPartitionKeyFieldsException.class})
    public ResponseEntity<Problem> handleUnprocessableEntityResponses(final NakadiBaseException exception,
                                                                      final NativeWebRequest request) {
        LOG.debug(exception.getMessage());
        return create(Problem.valueOf(UNPROCESSABLE_ENTITY, exception.getMessage()), request);
    }
}

