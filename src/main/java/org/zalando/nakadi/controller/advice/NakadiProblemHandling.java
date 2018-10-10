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
import org.zalando.nakadi.exceptions.runtime.CompactionException;
import org.zalando.nakadi.exceptions.runtime.ConflictException;
import org.zalando.nakadi.exceptions.runtime.CursorConversionException;
import org.zalando.nakadi.exceptions.runtime.CursorsAreEmptyException;
import org.zalando.nakadi.exceptions.runtime.DbWriteOperationsBlockedException;
import org.zalando.nakadi.exceptions.runtime.DuplicatedEventTypeNameException;
import org.zalando.nakadi.exceptions.runtime.DuplicatedStorageException;
import org.zalando.nakadi.exceptions.runtime.EnrichmentException;
import org.zalando.nakadi.exceptions.runtime.ErrorGettingCursorTimeLagException;
import org.zalando.nakadi.exceptions.runtime.EventTypeDeletionException;
import org.zalando.nakadi.exceptions.runtime.EventTypeOptionsValidationException;
import org.zalando.nakadi.exceptions.runtime.EventTypeUnavailableException;
import org.zalando.nakadi.exceptions.runtime.FeatureNotAvailableException;
import org.zalando.nakadi.exceptions.runtime.ForbiddenOperationException;
import org.zalando.nakadi.exceptions.runtime.IllegalClientIdException;
import org.zalando.nakadi.exceptions.runtime.InconsistentStateException;
import org.zalando.nakadi.exceptions.runtime.InternalNakadiException;
import org.zalando.nakadi.exceptions.runtime.InvalidCursorOperation;
import org.zalando.nakadi.exceptions.runtime.InvalidEventTypeException;
import org.zalando.nakadi.exceptions.runtime.InvalidLimitException;
import org.zalando.nakadi.exceptions.runtime.InvalidPartitionKeyFieldsException;
import org.zalando.nakadi.exceptions.runtime.InvalidStreamIdException;
import org.zalando.nakadi.exceptions.runtime.InvalidVersionNumberException;
import org.zalando.nakadi.exceptions.runtime.LimitReachedException;
import org.zalando.nakadi.exceptions.runtime.NakadiBaseException;
import org.zalando.nakadi.exceptions.runtime.NakadiRuntimeException;
import org.zalando.nakadi.exceptions.runtime.NoStreamingSlotsAvailable;
import org.zalando.nakadi.exceptions.runtime.NoSuchEventTypeException;
import org.zalando.nakadi.exceptions.runtime.NoSuchPartitionStrategyException;
import org.zalando.nakadi.exceptions.runtime.NoSuchSchemaException;
import org.zalando.nakadi.exceptions.runtime.NoSuchStorageException;
import org.zalando.nakadi.exceptions.runtime.NoSuchSubscriptionException;
import org.zalando.nakadi.exceptions.runtime.NotFoundException;
import org.zalando.nakadi.exceptions.runtime.PartitioningException;
import org.zalando.nakadi.exceptions.runtime.RepositoryProblemException;
import org.zalando.nakadi.exceptions.runtime.RequestInProgressException;
import org.zalando.nakadi.exceptions.runtime.ServiceTemporarilyUnavailableException;
import org.zalando.nakadi.exceptions.runtime.StorageIsUsedException;
import org.zalando.nakadi.exceptions.runtime.TimeLagStatsTimeoutException;
import org.zalando.nakadi.exceptions.runtime.TimelineException;
import org.zalando.nakadi.exceptions.runtime.TooManyPartitionsException;
import org.zalando.nakadi.exceptions.runtime.TopicCreationException;
import org.zalando.nakadi.exceptions.runtime.UnableProcessException;
import org.zalando.nakadi.exceptions.runtime.UnknownOperationException;
import org.zalando.nakadi.exceptions.runtime.UnknownStorageTypeException;
import org.zalando.nakadi.exceptions.runtime.UnprocessableEntityException;
import org.zalando.nakadi.exceptions.runtime.UnprocessableSubscriptionException;
import org.zalando.nakadi.exceptions.runtime.ValidationException;
import org.zalando.nakadi.exceptions.runtime.WrongInitialCursorsException;
import org.zalando.nakadi.exceptions.runtime.WrongStreamParametersException;
import org.zalando.nakadi.problem.ValidationProblem;
import org.zalando.problem.Problem;
import org.zalando.problem.spring.web.advice.ProblemHandling;

import javax.annotation.Priority;

import static org.zalando.problem.Status.BAD_REQUEST;
import static org.zalando.problem.Status.CONFLICT;
import static org.zalando.problem.Status.FORBIDDEN;
import static org.zalando.problem.Status.INTERNAL_SERVER_ERROR;
import static org.zalando.problem.Status.NOT_FOUND;
import static org.zalando.problem.Status.NOT_IMPLEMENTED;
import static org.zalando.problem.Status.REQUEST_TIMEOUT;
import static org.zalando.problem.Status.SERVICE_UNAVAILABLE;
import static org.zalando.problem.Status.TOO_MANY_REQUESTS;
import static org.zalando.problem.Status.UNPROCESSABLE_ENTITY;

@Priority(20)
@ControllerAdvice
public class NakadiProblemHandling implements ProblemHandling {

    private static final Logger LOG = LoggerFactory.getLogger(NakadiProblemHandling.class);
    static final String INVALID_CURSOR_MESSAGE = "invalid consumed_offset or partition";

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

    public String generateErrorTraceId() {
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

    @ExceptionHandler(BlockedException.class)
    public ResponseEntity<Problem> handleBlockedException(final BlockedException exception,
                                                          final NativeWebRequest request) {
        LOG.debug(exception.getMessage());
        return create(Problem.valueOf(FORBIDDEN, exception.getMessage()), request);
    }

    @ExceptionHandler(CompactionException.class)
    public ResponseEntity<Problem> handleCompactionException(final CompactionException exception,
                                                             final NativeWebRequest request) {
        LOG.debug(exception.getMessage());
        return create(Problem.valueOf(UNPROCESSABLE_ENTITY, exception.getMessage()), request);
    }

    @ExceptionHandler(ConflictException.class)
    public ResponseEntity<Problem> handleConflictException(final ConflictException exception,
                                                           final NativeWebRequest request) {
        LOG.debug(exception.getMessage(), exception);
        return create(Problem.valueOf(CONFLICT, exception.getMessage()), request);
    }

    @ExceptionHandler(CursorsAreEmptyException.class)
    public ResponseEntity<Problem> handleCursorsAreEmptyException(final RuntimeException exception,
                                                                  final NativeWebRequest request) {
        LOG.debug(exception.getMessage(), exception);
        return create(Problem.valueOf(UNPROCESSABLE_ENTITY, exception.getMessage()), request);
    }

    @ExceptionHandler(CursorConversionException.class)
    public ResponseEntity<Problem> handleCursorConversionException(final CursorConversionException exception,
                                                                   final NativeWebRequest request) {
        LOG.error(exception.getMessage(), exception);
        return create(Problem.valueOf(UNPROCESSABLE_ENTITY, exception.getMessage()), request);
    }

    @ExceptionHandler(DbWriteOperationsBlockedException.class)
    public ResponseEntity<Problem> handleDbWriteOperationsBlockedException(
            final DbWriteOperationsBlockedException exception, final NativeWebRequest request) {
        LOG.warn(exception.getMessage());
        return create(Problem.valueOf(SERVICE_UNAVAILABLE,
                "Database is currently in read-only mode"), request);
    }

    @ExceptionHandler(DuplicatedEventTypeNameException.class)
    public ResponseEntity<Problem> handleDuplicatedEventTypeNameException(
            final DuplicatedEventTypeNameException exception,
            final NativeWebRequest request) {
        LOG.debug(exception.getMessage(), exception);
        return create(Problem.valueOf(CONFLICT, exception.getMessage()), request);
    }

    @ExceptionHandler(DuplicatedStorageException.class)
    public ResponseEntity<Problem> handleDuplicatedStorageException(
            final DuplicatedStorageException exception,
            final NativeWebRequest request) {
        LOG.debug(exception.getMessage());
        return create(Problem.valueOf(CONFLICT, exception.getMessage()), request);
    }

    @ExceptionHandler(EnrichmentException.class)
    public ResponseEntity<Problem> handleEnrichmentException(final EnrichmentException exception,
                                                             final NativeWebRequest request) {
        LOG.debug(exception.getMessage());
        return create(Problem.valueOf(UNPROCESSABLE_ENTITY, exception.getMessage()), request);
    }

    @ExceptionHandler(ErrorGettingCursorTimeLagException.class)
    public ResponseEntity<Problem> handleErrorGettingCursorTimeLagException(
            final ErrorGettingCursorTimeLagException exception,
            final NativeWebRequest request) {
        LOG.debug(exception.getMessage(), exception);
        return create(Problem.valueOf(UNPROCESSABLE_ENTITY, exception.getMessage()), request);
    }

    @ExceptionHandler(EventTypeDeletionException.class)
    public ResponseEntity<Problem> handleEventTypeDeletionException(final EventTypeDeletionException exception,
                                                                    final NativeWebRequest request) {
        LOG.debug(exception.getMessage(), exception);
        return create(Problem.valueOf(INTERNAL_SERVER_ERROR, exception.getMessage()), request);
    }

    @ExceptionHandler(EventTypeOptionsValidationException.class)
    public ResponseEntity<Problem> handleEventTypeOptionsValidationException(
            final EventTypeOptionsValidationException exception,
            final NativeWebRequest request) {
        LOG.debug(exception.getMessage(), exception);
        return create(Problem.valueOf(UNPROCESSABLE_ENTITY, exception.getMessage()), request);
    }

    @ExceptionHandler(EventTypeUnavailableException.class)
    public ResponseEntity<Problem> handleEventTypeUnavailableException(final EventTypeUnavailableException exception,
                                                                       final NativeWebRequest request) {
        LOG.debug(exception.getMessage(), exception);
        return create(Problem.valueOf(SERVICE_UNAVAILABLE, exception.getMessage()), request);
    }

    @ExceptionHandler(FeatureNotAvailableException.class)
    public ResponseEntity<Problem> handleFeatureNotAvailableException(
            final FeatureNotAvailableException ex,
            final NativeWebRequest request) {
        LOG.debug(ex.getMessage());
        return create(Problem.valueOf(NOT_IMPLEMENTED, ex.getMessage()), request);
    }

    @ExceptionHandler(IllegalClientIdException.class)
    public ResponseEntity<Problem> handleIllegalClientIdException(final IllegalClientIdException exception,
                                                                  final NativeWebRequest request) {
        return create(Problem.valueOf(FORBIDDEN, exception.getMessage()), request);
    }

    @ExceptionHandler(InconsistentStateException.class)
    public ResponseEntity<Problem> handleInconsistentStateExcetpion(final InconsistentStateException exception,
                                                                    final NativeWebRequest request) {
        LOG.debug(exception.getMessage(), exception);
        return create(Problem.valueOf(SERVICE_UNAVAILABLE, exception.getMessage()), request);
    }

    @ExceptionHandler(InternalNakadiException.class)
    public ResponseEntity<Problem> handleInternalNakadiException(final InternalNakadiException exception,
                                                                 final NativeWebRequest request) {
        LOG.debug(exception.getMessage(), exception);
        return create(Problem.valueOf(INTERNAL_SERVER_ERROR, exception.getMessage()), request);
    }

    @ExceptionHandler(InvalidCursorOperation.class)
    public ResponseEntity<?> handleInvalidCursorOperation(final InvalidCursorOperation exception,
                                                          final NativeWebRequest request) {
        LOG.debug("User provided invalid cursor for operation. Reason: " + exception.getReason(), exception);
        return create(Problem.valueOf(UNPROCESSABLE_ENTITY, INVALID_CURSOR_MESSAGE), request);
    }

    @ExceptionHandler(InvalidEventTypeException.class)
    public ResponseEntity<Problem> handleInvalidEventTypeException(final InvalidEventTypeException exception,
                                                                   final NativeWebRequest request) {
        LOG.debug(exception.getMessage(), exception);
        return create(Problem.valueOf(UNPROCESSABLE_ENTITY, exception.getMessage()), request);
    }

    @ExceptionHandler(InvalidLimitException.class)
    public ResponseEntity<Problem> handleInvalidLimitException(
            final InvalidLimitException exception,
            final NativeWebRequest request) {
        LOG.debug(exception.getMessage());
        return create(Problem.valueOf(BAD_REQUEST, exception.getMessage()), request);
    }

    @ExceptionHandler(InvalidPartitionKeyFieldsException.class)
    public ResponseEntity<Problem> handleInvalidPartitionKeyFieldsException(
            final InvalidPartitionKeyFieldsException exception,
            final NativeWebRequest request) {
        LOG.debug(exception.getMessage());
        return create(Problem.valueOf(UNPROCESSABLE_ENTITY, exception.getMessage()), request);
    }

    @ExceptionHandler(InvalidStreamIdException.class)
    public ResponseEntity<Problem> handleInvalidStreamIdException(final InvalidStreamIdException exception,
                                                                  final NativeWebRequest request) {
        LOG.warn("Stream id {} is not found: {}", exception.getStreamId(), exception.getMessage());
        return create(Problem.valueOf(UNPROCESSABLE_ENTITY, exception.getMessage()), request);
    }

    @ExceptionHandler(InvalidVersionNumberException.class)
    public ResponseEntity<Problem> handleInvalidVersionNumberException(
            final InvalidVersionNumberException exception,
            final NativeWebRequest request) {
        LOG.debug(exception.getMessage());
        return create(Problem.valueOf(BAD_REQUEST, exception.getMessage()), request);
    }

    @ExceptionHandler(LimitReachedException.class)
    public ResponseEntity<Problem> handleLimitReachedException(
            final ServiceTemporarilyUnavailableException exception, final NativeWebRequest request) {
        LOG.warn(exception.getMessage());
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

    @ExceptionHandler(NotFoundException.class)
    public ResponseEntity<Problem> handleNotFoundException(final NotFoundException exception,
                                                           final NativeWebRequest request) {
        LOG.error(exception.getMessage(), exception);
        return create(Problem.valueOf(NOT_FOUND, exception.getMessage()), request);
    }

    @ExceptionHandler(NoStreamingSlotsAvailable.class)
    public ResponseEntity<Problem> handleNoStreamingSlotsAvailable(final NoStreamingSlotsAvailable exception,
                                                                   final NativeWebRequest request) {
        LOG.debug(exception.getMessage());
        return create(Problem.valueOf(CONFLICT, exception.getMessage()), request);
    }

    @ExceptionHandler(NoSuchEventTypeException.class)
    public ResponseEntity<Problem> handleNoSuchEventTypeException(final NoSuchEventTypeException exception,
                                                                  final NativeWebRequest request) {
        LOG.debug(exception.getMessage());
        return create(Problem.valueOf(NOT_FOUND, exception.getMessage()), request);
    }

    @ExceptionHandler(NoSuchPartitionStrategyException.class)
    public ResponseEntity<Problem> handleNoSuchPartitionStrategyException(
            final NoSuchPartitionStrategyException exception,
            final NativeWebRequest request) {
        LOG.debug(exception.getMessage());
        return create(Problem.valueOf(UNPROCESSABLE_ENTITY, exception.getMessage()), request);
    }

    @ExceptionHandler(NoSuchSchemaException.class)
    public ResponseEntity<Problem> handleNoSuchSchemaException(final NoSuchSchemaException exception,
                                                               final NativeWebRequest request) {
        LOG.debug(exception.getMessage());
        return create(Problem.valueOf(NOT_FOUND, exception.getMessage()), request);
    }

    @ExceptionHandler(NoSuchStorageException.class)
    public ResponseEntity<Problem> handleNoSuchStorageException(
            final NoSuchStorageException exception,
            final NativeWebRequest request) {
        LOG.debug(exception.getMessage());
        return create(Problem.valueOf(NOT_FOUND, exception.getMessage()), request);
    }

    @ExceptionHandler(NoSuchSubscriptionException.class)
    public ResponseEntity<Problem> handleNoSuchSubscriptionException(final NoSuchSubscriptionException exception,
                                                                     final NativeWebRequest request) {
        LOG.debug(exception.getMessage());
        return create(Problem.valueOf(NOT_FOUND, exception.getMessage()), request);
    }

    @ExceptionHandler(PartitioningException.class)
    public ResponseEntity<Problem> handlePartitioningException(final PartitioningException exception,
                                                               final NativeWebRequest request) {
        LOG.debug(exception.getMessage());
        return create(Problem.valueOf(UNPROCESSABLE_ENTITY, exception.getMessage()), request);
    }

    @ExceptionHandler(RepositoryProblemException.class)
    public ResponseEntity<Problem> handleRepositoryProblemException(final RepositoryProblemException exception,
                                                                    final NativeWebRequest request) {
        LOG.error("Repository problem occurred", exception);
        return create(Problem.valueOf(SERVICE_UNAVAILABLE, exception.getMessage()), request);
    }

    @ExceptionHandler(RequestInProgressException.class)
    public ResponseEntity<Problem> handleRequestInProgressException(final RequestInProgressException exception,
                                                                    final NativeWebRequest request) {
        LOG.debug(exception.getMessage(), exception);
        return create(Problem.valueOf(CONFLICT, exception.getMessage()), request);
    }

    @ExceptionHandler(ServiceTemporarilyUnavailableException.class)
    public ResponseEntity<Problem> handleServiceTemporarilyUnavailableException(
            final ServiceTemporarilyUnavailableException exception, final NativeWebRequest request) {
        LOG.error(exception.getMessage(), exception);
        return create(Problem.valueOf(SERVICE_UNAVAILABLE, exception.getMessage()), request);
    }

    @ExceptionHandler(StorageIsUsedException.class)
    public ResponseEntity<Problem> handleStorageIsUsedException(
            final StorageIsUsedException exception,
            final NativeWebRequest request) {
        LOG.debug(exception.getMessage());
        return create(Problem.valueOf(FORBIDDEN, exception.getMessage()), request);
    }

    @ExceptionHandler(TimeLagStatsTimeoutException.class)
    public ResponseEntity<Problem> handleTimeLagStatsTimeoutException(final TimeLagStatsTimeoutException exception,
                                                                      final NativeWebRequest request) {
        LOG.warn(exception.getMessage());
        return create(Problem.valueOf(REQUEST_TIMEOUT, exception.getMessage()), request);
    }

    @ExceptionHandler(TimelineException.class)
    public ResponseEntity<Problem> handleTimelineException(final TimelineException exception,
                                                           final NativeWebRequest request) {
        LOG.error(exception.getMessage(), exception);
        final Throwable cause = exception.getCause();
        if (cause instanceof InternalNakadiException) {
            return create(Problem.valueOf(INTERNAL_SERVER_ERROR, exception.getMessage()), request);
        }
        return create(Problem.valueOf(SERVICE_UNAVAILABLE, exception.getMessage()), request);
    }

    @ExceptionHandler(TooManyPartitionsException.class)
    public ResponseEntity<Problem> handleTooManyPartitionsException(final TooManyPartitionsException exception,
                                                                    final NativeWebRequest request) {
        LOG.debug("Error occurred when working with subscriptions", exception);
        return create(Problem.valueOf(UNPROCESSABLE_ENTITY, exception.getMessage()), request);
    }

    @ExceptionHandler(TopicCreationException.class)
    public ResponseEntity<Problem> handleTopicCreationException(final TopicCreationException exception,
                                                                final NativeWebRequest request) {
        LOG.error(exception.getMessage(), exception);
        return create(Problem.valueOf(SERVICE_UNAVAILABLE, exception.getMessage()), request);
    }

    @ExceptionHandler(UnableProcessException.class)
    public ResponseEntity<Problem> handleUnableProcessException(final UnableProcessException exception,
                                                                final NativeWebRequest request) {
        LOG.debug(exception.getMessage(), exception);
        return create(Problem.valueOf(UNPROCESSABLE_ENTITY, exception.getMessage()), request);
    }

    @ExceptionHandler(UnknownOperationException.class)
    public ResponseEntity<Problem> handleUnknownOperationException(final RuntimeException exception,
                                                                   final NativeWebRequest request) {
        LOG.error(exception.getMessage(), exception);
        return create(Problem.valueOf(SERVICE_UNAVAILABLE, "There was a problem processing your request."), request);
    }

    @ExceptionHandler(UnknownStorageTypeException.class)
    public ResponseEntity<Problem> handleUnknownStorageTypeException(
            final UnknownStorageTypeException exception,
            final NativeWebRequest request) {
        LOG.debug(exception.getMessage());
        return create(Problem.valueOf(UNPROCESSABLE_ENTITY, exception.getMessage()), request);
    }

    @ExceptionHandler(UnprocessableEntityException.class)
    public ResponseEntity<Problem> handleUnprocessableEntityException(
            final UnprocessableEntityException exception,
            final NativeWebRequest request) {
        LOG.debug(exception.getMessage());
        return create(Problem.valueOf(UNPROCESSABLE_ENTITY, exception.getMessage()), request);
    }

    @ExceptionHandler(UnprocessableSubscriptionException.class)
    public ResponseEntity<Problem> handleUnprocessableSubscriptionException(
            final UnprocessableSubscriptionException exception,
            final NativeWebRequest request) {
        LOG.debug(exception.getMessage());
        return create(Problem.valueOf(UNPROCESSABLE_ENTITY, exception.getMessage()), request);
    }

    @ExceptionHandler(WrongInitialCursorsException.class)
    public ResponseEntity<Problem> handleWrongInitialCursorsException(final WrongInitialCursorsException exception,
                                                                      final NativeWebRequest request) {
        LOG.debug("Error occurred when working with subscriptions", exception);
        return create(Problem.valueOf(UNPROCESSABLE_ENTITY, exception.getMessage()), request);
    }

    @ExceptionHandler(WrongStreamParametersException.class)
    public ResponseEntity<Problem> handleWrongStreamParametersException(final WrongStreamParametersException exception,
                                                                        final NativeWebRequest request) {
        LOG.debug(exception.getMessage(), exception);
        return create(Problem.valueOf(UNPROCESSABLE_ENTITY, exception.getMessage()), request);
    }

    @ExceptionHandler(ValidationException.class)
    public ResponseEntity<Problem> handleValidationException(final ValidationException exception,
                                                             final NativeWebRequest request) {
        return create(new ValidationProblem(exception.getErrors()), request);
    }

    @ExceptionHandler(ForbiddenOperationException.class)
    public ResponseEntity<Problem> handleForbiddenOperationException(final ForbiddenOperationException exception,
                                                                     final NativeWebRequest request) {
        LOG.error(exception.getMessage());
        return create(Problem.valueOf(FORBIDDEN, exception.getMessage()), request);
    }
}
