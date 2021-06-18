package org.zalando.nakadi;

import org.json.JSONException;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ControllerAdvice;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.context.request.NativeWebRequest;
import org.zalando.nakadi.exceptions.runtime.EnrichmentException;
import org.zalando.nakadi.exceptions.runtime.EventTypeTimeoutException;
import org.zalando.nakadi.exceptions.runtime.InvalidPartitionKeyFieldsException;
import org.zalando.nakadi.exceptions.runtime.NakadiBaseException;
import org.zalando.nakadi.exceptions.runtime.PartitioningException;
import org.zalando.nakadi.exceptions.runtime.PublishEventOwnershipException;
import org.zalando.problem.Problem;
import org.zalando.problem.Status;
import org.zalando.problem.spring.web.advice.AdviceTrait;

import javax.annotation.Priority;

@Priority(10)
@ControllerAdvice(assignableTypes = EventPublishingController.class)
public class EventPublishingExceptionHandler implements AdviceTrait {

    @ExceptionHandler(EventTypeTimeoutException.class)
    public ResponseEntity<Problem> handleEventTypeTimeoutException(final EventTypeTimeoutException exception,
                                                                   final NativeWebRequest request) {
        AdviceTrait.LOG.error(exception.getMessage());
        return create(Problem.valueOf(Status.SERVICE_UNAVAILABLE, exception.getMessage()), request);
    }

    @ExceptionHandler(JSONException.class)
    public ResponseEntity<Problem> handleJSONException(final JSONException exception,
                                                       final NativeWebRequest request) {
        if (exception.getCause() == null) {
            return create(Problem.valueOf(Status.BAD_REQUEST,
                    "Error occurred when parsing event(s). " + exception.getMessage()), request);
        }
        return create(Problem.valueOf(Status.BAD_REQUEST), request);
    }

    @ExceptionHandler({EnrichmentException.class,
            PartitioningException.class,
            InvalidPartitionKeyFieldsException.class})
    public ResponseEntity<Problem> handleUnprocessableEntityResponses(final NakadiBaseException exception,
                                                                      final NativeWebRequest request) {
        AdviceTrait.LOG.debug(exception.getMessage());
        return create(Problem.valueOf(Status.UNPROCESSABLE_ENTITY, exception.getMessage()), request);
    }

    @ExceptionHandler(PublishEventOwnershipException.class)
    public ResponseEntity<Problem> handlePublishEventOwnershipResponses(final NakadiBaseException exception,
                                                                        final NativeWebRequest request) {
        AdviceTrait.LOG.debug(exception.getMessage());
        return create(Problem.valueOf(Status.FORBIDDEN, exception.getMessage()), request);
    }
}

