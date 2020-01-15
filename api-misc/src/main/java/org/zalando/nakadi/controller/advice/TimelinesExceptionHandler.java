package org.zalando.nakadi.controller.advice;

import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ControllerAdvice;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.context.request.NativeWebRequest;
import org.zalando.nakadi.controller.TimelinesController;
import org.zalando.nakadi.exceptions.runtime.ConflictException;
import org.zalando.nakadi.exceptions.runtime.InternalNakadiException;
import org.zalando.nakadi.exceptions.runtime.NakadiBaseException;
import org.zalando.nakadi.exceptions.runtime.NotFoundException;
import org.zalando.nakadi.exceptions.runtime.TimelineException;
import org.zalando.nakadi.exceptions.runtime.TimelinesNotSupportedException;
import org.zalando.nakadi.exceptions.runtime.UnableProcessException;
import org.zalando.problem.Problem;
import org.zalando.problem.spring.web.advice.AdviceTrait;

import javax.annotation.Priority;

import static org.zalando.problem.Status.CONFLICT;
import static org.zalando.problem.Status.INTERNAL_SERVER_ERROR;
import static org.zalando.problem.Status.NOT_FOUND;
import static org.zalando.problem.Status.SERVICE_UNAVAILABLE;
import static org.zalando.problem.Status.UNPROCESSABLE_ENTITY;

@Priority(10)
@ControllerAdvice(assignableTypes = TimelinesController.class)
public class TimelinesExceptionHandler implements AdviceTrait {

    @ExceptionHandler(NotFoundException.class)
    public ResponseEntity<Problem> notFound(final NotFoundException exception, final NativeWebRequest request) {
        AdviceTrait.LOG.debug(exception.getMessage());
        return create(Problem.valueOf(NOT_FOUND, exception.getMessage()), request);
    }

    @ExceptionHandler(ConflictException.class)
    public ResponseEntity<Problem> handleConflictException(final ConflictException exception,
                                                           final NativeWebRequest request) {
        AdviceTrait.LOG.debug(exception.getMessage());
        return create(Problem.valueOf(CONFLICT, exception.getMessage()), request);
    }

    @ExceptionHandler(TimelineException.class)
    public ResponseEntity<Problem> handleTimelineException(final TimelineException exception,
                                                           final NativeWebRequest request) {
        AdviceTrait.LOG.error(exception.getMessage(), exception);
        final Throwable cause = exception.getCause();
        if (cause instanceof InternalNakadiException) {
            return create(Problem.valueOf(INTERNAL_SERVER_ERROR, exception.getMessage()), request);
        }
        return create(Problem.valueOf(SERVICE_UNAVAILABLE, exception.getMessage()), request);
    }

    @ExceptionHandler({UnableProcessException.class, TimelinesNotSupportedException.class})
    public ResponseEntity<Problem> handleUnprocessableEntityResponse(final NakadiBaseException exception,
                                                                     final NativeWebRequest request) {
        AdviceTrait.LOG.debug(exception.getMessage());
        return create(Problem.valueOf(UNPROCESSABLE_ENTITY, exception.getMessage()), request);
    }
}
