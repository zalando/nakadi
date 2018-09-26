package org.zalando.nakadi.controller;

import com.fasterxml.jackson.databind.JsonMappingException;
import com.google.common.base.CaseFormat;
import org.apache.commons.lang3.RandomStringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.ResponseEntity;
import org.springframework.http.converter.HttpMessageNotReadableException;
import org.springframework.web.bind.annotation.ControllerAdvice;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.context.request.NativeWebRequest;
import org.zalando.nakadi.exceptions.runtime.AccessDeniedException;
import org.zalando.nakadi.exceptions.runtime.DbWriteOperationsBlockedException;
import org.zalando.nakadi.exceptions.runtime.FeatureNotAvailableException;
import org.zalando.nakadi.exceptions.runtime.IllegalClientIdException;
import org.zalando.nakadi.exceptions.runtime.InternalNakadiException;
import org.zalando.nakadi.exceptions.runtime.InvalidLimitException;
import org.zalando.nakadi.exceptions.runtime.InvalidVersionNumberException;
import org.zalando.nakadi.exceptions.runtime.LimitReachedException;
import org.zalando.nakadi.exceptions.runtime.NakadiBaseException;
import org.zalando.nakadi.exceptions.runtime.NakadiRuntimeException;
import org.zalando.nakadi.exceptions.runtime.NoSuchEventTypeException;
import org.zalando.nakadi.exceptions.runtime.NoSuchSubscriptionException;
import org.zalando.nakadi.exceptions.runtime.RepositoryProblemException;
import org.zalando.nakadi.exceptions.runtime.ServiceTemporarilyUnavailableException;
import org.zalando.nakadi.exceptions.runtime.UnprocessableEntityException;
import org.zalando.problem.MoreStatus;
import org.zalando.problem.Problem;
import org.zalando.problem.spring.web.advice.ProblemHandling;
import org.zalando.problem.spring.web.advice.Responses;

import javax.ws.rs.core.Response;

import static javax.ws.rs.core.Response.Status.BAD_REQUEST;
import static javax.ws.rs.core.Response.Status.INTERNAL_SERVER_ERROR;
import static javax.ws.rs.core.Response.Status.NOT_FOUND;
import static javax.ws.rs.core.Response.Status.NOT_IMPLEMENTED;
import static org.zalando.problem.MoreStatus.UNPROCESSABLE_ENTITY;


@ControllerAdvice
public final class ExceptionHandling implements ProblemHandling {

    private static final Logger LOG = LoggerFactory.getLogger(ExceptionHandling.class);

    @Override
    public String formatFieldName(final String fieldName) {
        return CaseFormat.LOWER_CAMEL.to(CaseFormat.LOWER_UNDERSCORE, fieldName);
    }

    @Override
    @ExceptionHandler
    public ResponseEntity<Problem> handleThrowable(final Throwable throwable, final NativeWebRequest request) {
        final String errorTraceId = generateErrorTraceId();
        LOG.error("InternalServerError (" + errorTraceId + "):", throwable);
        return Responses.create(Response.Status.INTERNAL_SERVER_ERROR, "An internal error happened. Please report it. ("
                + errorTraceId + ")", request);
    }

    private String generateErrorTraceId() {
        return "ETI" + RandomStringUtils.randomAlphanumeric(24);
    }

    @Override
    @ExceptionHandler
    public ResponseEntity<Problem> handleMessageNotReadableException(final HttpMessageNotReadableException exception,
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
        return Responses.create(Response.Status.BAD_REQUEST, message, request);
    }

    @ExceptionHandler(AccessDeniedException.class)
    public ResponseEntity<Problem> accessDeniedException(final AccessDeniedException exception,
                                                         final NativeWebRequest request) {
        return Responses.create(Response.Status.FORBIDDEN, exception.explain(), request);
    }

    @ExceptionHandler(IllegalClientIdException.class)
    public ResponseEntity<Problem> handleIllegalClientIdException(final IllegalClientIdException exception,
                                                                  final NativeWebRequest request) {
        return Responses.create(Response.Status.FORBIDDEN, exception.getMessage(), request);
    }

    @ExceptionHandler
    public ResponseEntity<Problem> handleExceptionWrapper(final NakadiRuntimeException exception,
                                                          final NativeWebRequest request) throws Exception {
        final Throwable cause = exception.getCause();
        if (cause instanceof InternalNakadiException) {
            return Responses.create(INTERNAL_SERVER_ERROR, exception.getMessage(), request);
        }
        throw exception.getException();
    }

    @ExceptionHandler(RepositoryProblemException.class)
    public ResponseEntity<Problem> handleRepositoryProblem(final RepositoryProblemException exception,
                                                           final NativeWebRequest request) {
        LOG.error("Repository problem occurred", exception);
        return Responses.create(Response.Status.SERVICE_UNAVAILABLE, exception.getMessage(), request);
    }

    @ExceptionHandler(NakadiBaseException.class)
    public ResponseEntity<Problem> handleInternalError(final NakadiBaseException exception,
                                                       final NativeWebRequest request) {
        LOG.error("Unexpected problem occurred", exception);
        return Responses.create(Response.Status.INTERNAL_SERVER_ERROR, exception.getMessage(), request);
    }

    @ExceptionHandler(ServiceTemporarilyUnavailableException.class)
    public ResponseEntity<Problem> handleServiceTemporarilyUnavailableException(
            final ServiceTemporarilyUnavailableException exception, final NativeWebRequest request) {
        LOG.error(exception.getMessage(), exception);
        return Responses.create(Response.Status.SERVICE_UNAVAILABLE, exception.getMessage(), request);
    }

    @ExceptionHandler(LimitReachedException.class)
    public ResponseEntity<Problem> handleLimitReachedException(
            final ServiceTemporarilyUnavailableException exception, final NativeWebRequest request) {
        LOG.warn(exception.getMessage());
        return Responses.create(MoreStatus.TOO_MANY_REQUESTS, exception.getMessage(), request);
    }

    @ExceptionHandler(DbWriteOperationsBlockedException.class)
    public ResponseEntity<Problem> handleDbWriteOperationsBlockedException(
            final DbWriteOperationsBlockedException exception, final NativeWebRequest request) {
        LOG.warn(exception.getMessage());
        return Responses.create(Response.Status.SERVICE_UNAVAILABLE,
                "Database is currently in read-only mode", request);
    }

    @ExceptionHandler(FeatureNotAvailableException.class)
    public ResponseEntity<Problem> handleFeatureNotAvailable(
            final FeatureNotAvailableException ex,
            final NativeWebRequest request) {
        LOG.debug(ex.getMessage());
        return Responses.create(Problem.valueOf(NOT_IMPLEMENTED, ex.getMessage()), request);
    }

    @ExceptionHandler(NoSuchEventTypeException.class)
    public ResponseEntity<Problem> handleNoSuchEventTypeException(final NoSuchEventTypeException exception,
                                                               final NativeWebRequest request) {
        LOG.debug(exception.getMessage());
        return Responses.create(NOT_FOUND, exception.getMessage(), request);
    }

    @ExceptionHandler(NoSuchSubscriptionException.class)
    public ResponseEntity<Problem> handleNoSuchSubscriptionException(final NoSuchSubscriptionException exception,
                                                                     final NativeWebRequest request) {
        LOG.debug(exception.getMessage());
        return Responses.create(NOT_FOUND, exception.getMessage(), request);
    }

    @ExceptionHandler(InvalidLimitException.class)
    public ResponseEntity<Problem> handleInvalidLimitException(
            final InvalidLimitException exception,
            final NativeWebRequest request) {
        LOG.debug(exception.getMessage());
        return Responses.create(BAD_REQUEST, exception.getMessage(), request);
    }

    @ExceptionHandler(InvalidVersionNumberException.class)
    public ResponseEntity<Problem> handleInvalidVersionNumberException(
            final InvalidVersionNumberException exception,
            final NativeWebRequest request) {
        LOG.debug(exception.getMessage());
        return Responses.create(BAD_REQUEST, exception.getMessage(), request);
    }

    @ExceptionHandler(UnprocessableEntityException.class)
    public ResponseEntity<Problem> handleUnprocessableEntityException(
            final UnprocessableEntityException exception,
            final NativeWebRequest request) {
        LOG.debug(exception.getMessage());
        return Responses.create(UNPROCESSABLE_ENTITY, exception.getMessage(), request);
    }
}
