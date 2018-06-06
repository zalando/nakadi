package org.zalando.nakadi.controller;

import com.fasterxml.jackson.databind.JsonMappingException;
import com.google.common.base.CaseFormat;
import org.apache.commons.lang3.RandomStringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.ResponseEntity;
import org.springframework.http.converter.HttpMessageNotReadableException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.context.request.NativeWebRequest;
import org.zalando.nakadi.exceptions.NakadiWrapperException;
import org.zalando.nakadi.exceptions.runtime.AccessDeniedException;
import org.zalando.nakadi.exceptions.runtime.CursorConversionException;
import org.zalando.nakadi.exceptions.runtime.CursorsAreEmptyException;
import org.zalando.nakadi.exceptions.runtime.DbWriteOperationsBlockedException;
import org.zalando.nakadi.exceptions.runtime.IllegalClientIdException;
import org.zalando.nakadi.exceptions.runtime.InternalNakadiException;
import org.zalando.nakadi.exceptions.runtime.LimitReachedException;
import org.zalando.nakadi.exceptions.runtime.NakadiRuntimeBaseException;
import org.zalando.nakadi.exceptions.runtime.NoSuchEventTypeException;
import org.zalando.nakadi.exceptions.runtime.RepositoryProblemException;
import org.zalando.nakadi.exceptions.runtime.ServiceTemporarilyUnavailableException;
import org.zalando.nakadi.exceptions.runtime.TimelineException;
import org.zalando.nakadi.exceptions.runtime.TopicCreationException;
import org.zalando.problem.Problem;
import org.zalando.problem.spring.web.advice.ProblemHandling;

import static org.zalando.problem.Status.BAD_REQUEST;
import static org.zalando.problem.Status.FORBIDDEN;
import static org.zalando.problem.Status.INTERNAL_SERVER_ERROR;
import static org.zalando.problem.Status.NOT_FOUND;
import static org.zalando.problem.Status.SERVICE_UNAVAILABLE;
import static org.zalando.problem.Status.TOO_MANY_REQUESTS;
import static org.zalando.problem.Status.UNPROCESSABLE_ENTITY;


public interface NakadiProblemHandling extends ProblemHandling {

    Logger LOG = LoggerFactory.getLogger(NakadiProblemHandling.class);

    @Override
    default String formatFieldName(final String fieldName) {
        return CaseFormat.LOWER_CAMEL.to(CaseFormat.LOWER_UNDERSCORE, fieldName);
    }

    @Override
    @ExceptionHandler
    default ResponseEntity<Problem> handleThrowable(final Throwable throwable, final NativeWebRequest request) {
        final String errorTraceId = generateErrorTraceId();
        LOG.error("InternalServerError (" + errorTraceId + "):", throwable);
        return create(Problem.valueOf(INTERNAL_SERVER_ERROR, "An internal error happened. Please report it. ("
                + errorTraceId + ")"), request);
    }

    default String generateErrorTraceId() {
        return "ETI" + RandomStringUtils.randomAlphanumeric(24);
    }

    @Override
    @ExceptionHandler
    default ResponseEntity<Problem> handleMessageNotReadableException(final HttpMessageNotReadableException exception,
                                                                      final NativeWebRequest request) {
        /*
        Unwrap nested JsonMappingException because the enclosing HttpMessageNotReadableException adds some ugly, Java
        class and stacktrace like information.
         */
        final Throwable mostSpecificCause = exception.getMostSpecificCause();
        final String message;
        if (mostSpecificCause instanceof JsonMappingException) {
            message = mostSpecificCause.getMessage();
        } else {
            message = exception.getMessage();
        }
        return create(Problem.valueOf(BAD_REQUEST, message), request);
    }

    @ExceptionHandler(AccessDeniedException.class)
    default ResponseEntity<Problem> handleAccessDeniedException(final AccessDeniedException exception,
                                                                final NativeWebRequest request) {
        return create(Problem.valueOf(FORBIDDEN, exception.explain()), request);
    }

    @ExceptionHandler(CursorsAreEmptyException.class)
    default ResponseEntity<Problem> handleCursorsUnavailableException(final RuntimeException exception,
                                                                      final NativeWebRequest request) {
        LOG.debug(exception.getMessage(), exception);
        return create(Problem.valueOf(UNPROCESSABLE_ENTITY, exception.getMessage()), request);
    }

    @ExceptionHandler(CursorConversionException.class)
    default ResponseEntity<Problem> handleCursorConversionException(final CursorConversionException exception,
                                                                    final NativeWebRequest request) {
        LOG.error(exception.getMessage(), exception);
        return create(Problem.valueOf(UNPROCESSABLE_ENTITY, exception.getMessage()), request);
    }

    @ExceptionHandler(DbWriteOperationsBlockedException.class)
    default ResponseEntity<Problem> handleDbWriteOperationsBlockedException(
            final DbWriteOperationsBlockedException exception, final NativeWebRequest request) {
        LOG.warn(exception.getMessage());
        return create(Problem.valueOf(SERVICE_UNAVAILABLE,
                "Database is currently in read-only mode"), request);
    }

    @ExceptionHandler(IllegalClientIdException.class)
    default ResponseEntity<Problem> handleIllegalClientIdException(final IllegalClientIdException exception,
                                                                   final NativeWebRequest request) {
        return create(Problem.valueOf(FORBIDDEN, exception.getMessage()), request);
    }

    @ExceptionHandler(LimitReachedException.class)
    default ResponseEntity<Problem> handleLimitReachedException(final ServiceTemporarilyUnavailableException exception,
                                                                final NativeWebRequest request) {
        LOG.warn(exception.getMessage());
        return create(Problem.valueOf(TOO_MANY_REQUESTS, exception.getMessage()), request);
    }

    @ExceptionHandler(NakadiRuntimeBaseException.class)
    default ResponseEntity<Problem> handleInternalErrorException(final NakadiRuntimeBaseException exception,
                                                                 final NativeWebRequest request) {
        LOG.error("Unexpected problem occurred", exception);
        return create(Problem.valueOf(INTERNAL_SERVER_ERROR, exception.getMessage()), request);
    }

    @ExceptionHandler(NoSuchEventTypeException.class)
    default ResponseEntity<Problem> handleNoSuchEventTypeException(final NoSuchEventTypeException exception,
                                                                   final NativeWebRequest request) {
        return create(Problem.valueOf(NOT_FOUND, exception.getMessage()), request);
    }

    @ExceptionHandler(RepositoryProblemException.class)
    default ResponseEntity<Problem> handleRepositoryProblemException(final RepositoryProblemException exception,
                                                                     final NativeWebRequest request) {
        LOG.error("Repository problem occurred", exception);
        return create(Problem.valueOf(SERVICE_UNAVAILABLE, exception.getMessage()), request);
    }

    @ExceptionHandler(ServiceTemporarilyUnavailableException.class)
    default ResponseEntity<Problem> handleServiceTemporarilyUnavailableException(
            final ServiceTemporarilyUnavailableException exception,
            final NativeWebRequest request) {
        LOG.error(exception.getMessage(), exception);
        return create(Problem.valueOf(SERVICE_UNAVAILABLE, exception.getMessage()), request);
    }

    @ExceptionHandler(TimelineException.class)
    default ResponseEntity<Problem> handleTimelineException(final TimelineException exception,
                                                            final NativeWebRequest request) {
        LOG.error(exception.getMessage(), exception);
        final Throwable cause = exception.getCause();
        if (cause instanceof InternalNakadiException) {
            final InternalNakadiException ne = (InternalNakadiException) cause;
            return create(Problem.valueOf(INTERNAL_SERVER_ERROR, ne.getMessage()), request);
        }
        return create(Problem.valueOf(SERVICE_UNAVAILABLE, exception.getMessage()), request);
    }

    @ExceptionHandler(TopicCreationException.class)
    default ResponseEntity<Problem> handleTopicCreationException(final TopicCreationException exception,
                                                                 final NativeWebRequest request) {
        LOG.error(exception.getMessage(), exception);
        return create(Problem.valueOf(SERVICE_UNAVAILABLE, exception.getMessage()), request);
    }

    @ExceptionHandler
    default ResponseEntity<Problem> handleNakadiRuntimeException(final NakadiWrapperException exception,
                                                                 final NativeWebRequest request) throws Exception {
        final Throwable cause = exception.getCause();
        if (cause instanceof InternalNakadiException) {
            final InternalNakadiException ne = (InternalNakadiException) cause;
            return create(Problem.valueOf(INTERNAL_SERVER_ERROR, ne.getMessage()), request);
        }
        throw exception.getException();
    }

}
