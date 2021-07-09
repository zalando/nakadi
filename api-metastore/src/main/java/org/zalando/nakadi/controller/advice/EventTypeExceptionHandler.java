package org.zalando.nakadi.controller.advice;

import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ControllerAdvice;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.context.request.NativeWebRequest;
import org.zalando.nakadi.controller.EventTypeController;
import org.zalando.nakadi.exceptions.runtime.AuthorizationSectionException;
import org.zalando.nakadi.exceptions.runtime.ConflictException;
import org.zalando.nakadi.exceptions.runtime.DuplicatedEventTypeNameException;
import org.zalando.nakadi.exceptions.runtime.EventTypeDeletionException;
import org.zalando.nakadi.exceptions.runtime.EventTypeOptionsValidationException;
import org.zalando.nakadi.exceptions.runtime.EventTypeUnavailableException;
import org.zalando.nakadi.exceptions.runtime.InvalidEventTypeException;
import org.zalando.nakadi.exceptions.runtime.NakadiBaseException;
import org.zalando.nakadi.exceptions.runtime.NoSuchPartitionStrategyException;
import org.zalando.nakadi.exceptions.runtime.TopicCreationException;
import org.zalando.nakadi.exceptions.runtime.UnableProcessException;
import org.zalando.nakadi.exceptions.runtime.WrongOwningApplicationException;
import org.zalando.problem.Problem;
import org.zalando.problem.spring.web.advice.AdviceTrait;

import javax.annotation.Priority;

import static org.zalando.problem.Status.CONFLICT;
import static org.zalando.problem.Status.INTERNAL_SERVER_ERROR;
import static org.zalando.problem.Status.SERVICE_UNAVAILABLE;
import static org.zalando.problem.Status.UNPROCESSABLE_ENTITY;

@Priority(10)
@ControllerAdvice(assignableTypes = EventTypeController.class)
public class EventTypeExceptionHandler implements AdviceTrait {

    @ExceptionHandler({InvalidEventTypeException.class,
            UnableProcessException.class,
            EventTypeOptionsValidationException.class,
            AuthorizationSectionException.class,
            NoSuchPartitionStrategyException.class,
            WrongOwningApplicationException.class,
    })
    public ResponseEntity<Problem> handleUnprocessableEntityResponses(final NakadiBaseException exception,
                                                                      final NativeWebRequest request) {
        AdviceTrait.LOG.debug(exception.getMessage());
        return create(Problem.valueOf(UNPROCESSABLE_ENTITY, exception.getMessage()), request);
    }

    @ExceptionHandler(EventTypeDeletionException.class)
    public ResponseEntity<Problem> handleEventTypeDeletionException(final EventTypeDeletionException exception,
                                                                    final NativeWebRequest request) {
        AdviceTrait.LOG.error(exception.getMessage(), exception);
        return create(Problem.valueOf(INTERNAL_SERVER_ERROR, exception.getMessage()), request);
    }

    @ExceptionHandler({DuplicatedEventTypeNameException.class, ConflictException.class})
    public ResponseEntity<Problem> handleConflictResponses(final NakadiBaseException exception,
                                                           final NativeWebRequest request) {
        AdviceTrait.LOG.debug(exception.getMessage());
        return create(Problem.valueOf(CONFLICT, exception.getMessage()), request);
    }

    @ExceptionHandler({EventTypeUnavailableException.class, TopicCreationException.class})
    public ResponseEntity<Problem> handleServiceUnavailableResponses(final NakadiBaseException exception,
                                                                     final NativeWebRequest request) {
        AdviceTrait.LOG.error(exception.getMessage(), exception);
        return create(Problem.valueOf(SERVICE_UNAVAILABLE, exception.getMessage()), request);
    }
}
