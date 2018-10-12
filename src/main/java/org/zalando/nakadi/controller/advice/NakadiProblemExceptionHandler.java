package org.zalando.nakadi.controller.advice;

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
import org.zalando.nakadi.exceptions.runtime.BlockedException;
import org.zalando.nakadi.exceptions.runtime.DbWriteOperationsBlockedException;
import org.zalando.nakadi.exceptions.runtime.FeatureNotAvailableException;
import org.zalando.nakadi.exceptions.runtime.ForbiddenOperationException;
import org.zalando.nakadi.exceptions.runtime.IllegalClientIdException;
import org.zalando.nakadi.exceptions.runtime.InternalNakadiException;
import org.zalando.nakadi.exceptions.runtime.InvalidLimitException;
import org.zalando.nakadi.exceptions.runtime.InvalidVersionNumberException;
import org.zalando.nakadi.exceptions.runtime.LimitReachedException;
import org.zalando.nakadi.exceptions.runtime.NakadiBaseException;
import org.zalando.nakadi.exceptions.runtime.NakadiRuntimeException;
import org.zalando.nakadi.exceptions.runtime.NoSuchEventTypeException;
import org.zalando.nakadi.exceptions.runtime.NoSuchSchemaException;
import org.zalando.nakadi.exceptions.runtime.NoSuchSubscriptionException;
import org.zalando.nakadi.exceptions.runtime.RepositoryProblemException;
import org.zalando.nakadi.exceptions.runtime.ServiceTemporarilyUnavailableException;
import org.zalando.nakadi.exceptions.runtime.UnprocessableEntityException;
import org.zalando.nakadi.exceptions.runtime.ValidationException;
import org.zalando.nakadi.problem.ValidationProblem;
import org.zalando.problem.Problem;
import org.zalando.problem.spring.web.advice.ProblemHandling;

import javax.annotation.Priority;

import static org.zalando.problem.Status.BAD_REQUEST;
import static org.zalando.problem.Status.FORBIDDEN;
import static org.zalando.problem.Status.INTERNAL_SERVER_ERROR;
import static org.zalando.problem.Status.NOT_FOUND;
import static org.zalando.problem.Status.NOT_IMPLEMENTED;
import static org.zalando.problem.Status.SERVICE_UNAVAILABLE;
import static org.zalando.problem.Status.TOO_MANY_REQUESTS;
import static org.zalando.problem.Status.UNPROCESSABLE_ENTITY;

@Priority(20)
@ControllerAdvice
public class NakadiProblemExceptionHandler implements ProblemHandling {

    private static final Logger LOG = LoggerFactory.getLogger(NakadiProblemExceptionHandler.class);

    @Override
    public String formatFieldName(final String fieldName) {
        return CaseFormat.LOWER_CAMEL.to(CaseFormat.LOWER_UNDERSCORE, fieldName);
    }

    @Override
    @ExceptionHandler
    public ResponseEntity<Problem> handleThrowable(final Throwable throwable, final NativeWebRequest request) {
        final String errorTraceId = generateErrorTraceId();
        LOG.error("InternalServerError (" + errorTraceId + "):", throwable);
        return create(Problem.valueOf(INTERNAL_SERVER_ERROR, "An internal error happened. Please report it. ("
                + errorTraceId + ")"), request);
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
        return create(Problem.valueOf(BAD_REQUEST, message), request);
    }

    @ExceptionHandler(AccessDeniedException.class)
    public ResponseEntity<Problem> handleAccessDeniedException(final AccessDeniedException exception,
                                                               final NativeWebRequest request) {
        return create(Problem.valueOf(FORBIDDEN, exception.explain()), request);
    }

    @ExceptionHandler(DbWriteOperationsBlockedException.class)
    public ResponseEntity<Problem> handleDbWriteOperationsBlockedException(
            final DbWriteOperationsBlockedException exception, final NativeWebRequest request) {
        LOG.warn(exception.getMessage());
        return create(Problem.valueOf(SERVICE_UNAVAILABLE,
                "Database is currently in read-only mode"), request);
    }

    @ExceptionHandler(FeatureNotAvailableException.class)
    public ResponseEntity<Problem> handleFeatureNotAvailableException(
            final FeatureNotAvailableException ex,
            final NativeWebRequest request) {
        LOG.debug(ex.getMessage());
        return create(Problem.valueOf(NOT_IMPLEMENTED, ex.getMessage()), request);
    }

    @ExceptionHandler(InternalNakadiException.class)
    public ResponseEntity<Problem> handleInternalNakadiException(final InternalNakadiException exception,
                                                                 final NativeWebRequest request) {
        LOG.error(exception.getMessage(), exception);
        return create(Problem.valueOf(INTERNAL_SERVER_ERROR, exception.getMessage()), request);
    }

    @ExceptionHandler({InvalidLimitException.class, InvalidVersionNumberException.class})
    public ResponseEntity<Problem> handleBadRequestResponses(final NakadiBaseException exception,
                                                             final NativeWebRequest request) {
        LOG.debug(exception.getMessage());
        return create(Problem.valueOf(BAD_REQUEST, exception.getMessage()), request);
    }

    @ExceptionHandler(LimitReachedException.class)
    public ResponseEntity<Problem> handleLimitReachedException(
            final ServiceTemporarilyUnavailableException exception, final NativeWebRequest request) {
        LOG.debug(exception.getMessage());
        return create(Problem.valueOf(TOO_MANY_REQUESTS, exception.getMessage()), request);
    }

    @ExceptionHandler(NakadiBaseException.class)
    public ResponseEntity<Problem> handleNakadiBaseException(final NakadiBaseException exception,
                                                             final NativeWebRequest request) {
        LOG.error("Unexpected problem occurred", exception);
        return create(Problem.valueOf(INTERNAL_SERVER_ERROR, exception.getMessage()), request);
    }

    @ExceptionHandler
    public ResponseEntity<Problem> handleNakadiRuntimeException(final NakadiRuntimeException exception,
                                                                final NativeWebRequest request) throws Exception {
        final Throwable cause = exception.getCause();
        if (cause instanceof InternalNakadiException) {
            return create(Problem.valueOf(INTERNAL_SERVER_ERROR, exception.getMessage()), request);
        }
        throw exception.getException();
    }

    @ExceptionHandler({NoSuchEventTypeException.class, NoSuchSchemaException.class, NoSuchSubscriptionException.class})
    public ResponseEntity<Problem> handleNotFoundResponses(final NakadiBaseException exception,
                                                           final NativeWebRequest request) {
        LOG.debug(exception.getMessage());
        return create(Problem.valueOf(NOT_FOUND, exception.getMessage()), request);
    }

    @ExceptionHandler(RepositoryProblemException.class)
    public ResponseEntity<Problem> handleRepositoryProblemException(final RepositoryProblemException exception,
                                                                    final NativeWebRequest request) {
        LOG.error("Repository problem occurred", exception);
        return create(Problem.valueOf(SERVICE_UNAVAILABLE, exception.getMessage()), request);
    }

    @ExceptionHandler(ServiceTemporarilyUnavailableException.class)
    public ResponseEntity<Problem> handleServiceTemporarilyUnavailableException(
            final ServiceTemporarilyUnavailableException exception, final NativeWebRequest request) {
        LOG.error(exception.getMessage(), exception);
        return create(Problem.valueOf(SERVICE_UNAVAILABLE, exception.getMessage()), request);
    }

    @ExceptionHandler(UnprocessableEntityException.class)
    public ResponseEntity<Problem> handleUnprocessableEntityException(
            final UnprocessableEntityException exception,
            final NativeWebRequest request) {
        LOG.debug(exception.getMessage());
        return create(Problem.valueOf(UNPROCESSABLE_ENTITY, exception.getMessage()), request);
    }

    @ExceptionHandler(ValidationException.class)
    public ResponseEntity<Problem> handleValidationException(final ValidationException exception,
                                                             final NativeWebRequest request) {
        return create(new ValidationProblem(exception.getErrors()), request);
    }

    @ExceptionHandler({ForbiddenOperationException.class, BlockedException.class, IllegalClientIdException.class})
    public ResponseEntity<Problem> handleForbiddenResponses(final NakadiBaseException exception,
                                                            final NativeWebRequest request) {
        LOG.debug(exception.getMessage());
        return create(Problem.valueOf(FORBIDDEN, exception.getMessage()), request);
    }
}
