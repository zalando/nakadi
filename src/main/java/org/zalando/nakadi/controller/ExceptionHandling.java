package org.zalando.nakadi.controller;

import com.fasterxml.jackson.databind.JsonMappingException;
import com.google.common.base.CaseFormat;
import javax.ws.rs.core.Response;
import org.apache.commons.lang3.RandomStringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.ResponseEntity;
import org.springframework.http.converter.HttpMessageNotReadableException;
import org.springframework.web.bind.annotation.ControllerAdvice;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.context.request.NativeWebRequest;
import org.zalando.nakadi.exceptions.IllegalClientIdException;
import org.zalando.nakadi.exceptions.IllegalScopeException;
import org.zalando.nakadi.exceptions.NakadiException;
import org.zalando.nakadi.exceptions.NakadiRuntimeException;
import org.zalando.nakadi.exceptions.TimelineException;
import org.zalando.nakadi.exceptions.TopicCreationException;
import org.zalando.nakadi.exceptions.runtime.AccessDeniedException;
import org.zalando.nakadi.exceptions.runtime.CursorConversionException;
import org.zalando.nakadi.exceptions.runtime.MyNakadiRuntimeException1;
import org.zalando.nakadi.exceptions.runtime.NoEventTypeException;
import org.zalando.nakadi.exceptions.runtime.RepositoryProblemException;
import org.zalando.nakadi.exceptions.runtime.ServiceTemporarilyUnavailableException;
import static org.zalando.problem.MoreStatus.UNPROCESSABLE_ENTITY;
import org.zalando.problem.Problem;
import org.zalando.problem.spring.web.advice.ProblemHandling;
import org.zalando.problem.spring.web.advice.Responses;


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

    @ExceptionHandler(NoEventTypeException.class)
    public ResponseEntity<Problem> noEventTypeException(final NoEventTypeException exception,
                                                        final NativeWebRequest request) {
        return Responses.create(Response.Status.NOT_FOUND, exception.getMessage(), request);
    }

    @ExceptionHandler(AccessDeniedException.class)
    public ResponseEntity<Problem> accessDeniedException(final AccessDeniedException exception,
                                                         final NativeWebRequest request) {
        return Responses.create(Response.Status.FORBIDDEN, exception.explain(), request);
    }

    @ExceptionHandler(IllegalScopeException.class)
    public ResponseEntity<Problem> handleIllegalScopeException(final IllegalScopeException exception,
                                                               final NativeWebRequest request) {
        return Responses.create(Response.Status.FORBIDDEN, exception.getMessage(), request);
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
        if (cause instanceof NakadiException) {
            final NakadiException ne = (NakadiException) cause;
            return Responses.create(ne.asProblem(), request);
        }
        throw exception.getException();
    }

    @ExceptionHandler(RepositoryProblemException.class)
    public ResponseEntity<Problem> handleRepositoryProblem(final RepositoryProblemException exception,
                                                           final NativeWebRequest request) {
        LOG.error("Repository problem occurred", exception);
        return Responses.create(Response.Status.SERVICE_UNAVAILABLE, exception.getMessage(), request);
    }

    @ExceptionHandler(MyNakadiRuntimeException1.class)
    public ResponseEntity<Problem> handleInternalError(final MyNakadiRuntimeException1 exception,
                                                       final NativeWebRequest request) {
        LOG.error("Unexpected problem occurred", exception);
        return Responses.create(Response.Status.INTERNAL_SERVER_ERROR, exception.getMessage(), request);
    }

    @ExceptionHandler(TimelineException.class)
    public ResponseEntity<Problem> handleTimelineException(final TimelineException exception,
                                                           final NativeWebRequest request) {
        LOG.error(exception.getMessage(), exception);
        final Throwable cause = exception.getCause();
        if (cause instanceof NakadiException) {
            final NakadiException ne = (NakadiException) cause;
            return Responses.create(ne.asProblem(), request);
        }
        return Responses.create(Response.Status.SERVICE_UNAVAILABLE, exception.getMessage(), request);
    }

    @ExceptionHandler(TopicCreationException.class)
    public ResponseEntity<Problem> handleTopicCreationException(final TopicCreationException exception,
                                                                final NativeWebRequest request) {
        LOG.error(exception.getMessage(), exception);
        return Responses.create(Response.Status.SERVICE_UNAVAILABLE, exception.getMessage(), request);
    }

    @ExceptionHandler(CursorConversionException.class)
    public ResponseEntity<Problem> handleCursorConversionException(final CursorConversionException exception,
                                                                   final NativeWebRequest request) {
        LOG.error(exception.getMessage(), exception);
        return Responses.create(UNPROCESSABLE_ENTITY, exception.getMessage(), request);
    }

    @ExceptionHandler(ServiceTemporarilyUnavailableException.class)
    public ResponseEntity<Problem> handleServiceTemporaryUnavailableException(
            final ServiceTemporarilyUnavailableException exception, final NativeWebRequest request) {
        LOG.error(exception.getMessage(), exception);
        return Responses.create(Response.Status.SERVICE_UNAVAILABLE, exception.getMessage(), request);
    }

}
