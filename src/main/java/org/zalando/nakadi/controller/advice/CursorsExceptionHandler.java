package org.zalando.nakadi.controller.advice;

import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.annotation.ControllerAdvice;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.context.request.NativeWebRequest;
import org.zalando.nakadi.controller.CursorsController;
import org.zalando.nakadi.exceptions.runtime.CursorsAreEmptyException;
import org.zalando.nakadi.exceptions.runtime.InvalidCursorException;
import org.zalando.nakadi.exceptions.runtime.InvalidStreamIdException;
import org.zalando.nakadi.exceptions.runtime.NoSuchEventTypeException;
import org.zalando.nakadi.exceptions.runtime.RequestInProgressException;
import org.zalando.nakadi.exceptions.runtime.UnableProcessException;
import org.zalando.nakadi.problem.ValidationProblem;
import org.zalando.problem.Problem;
import org.zalando.problem.spring.web.advice.AdviceTrait;

import javax.annotation.Priority;

import static org.zalando.problem.Status.CONFLICT;
import static org.zalando.problem.Status.UNPROCESSABLE_ENTITY;

@Priority(10)
@ControllerAdvice(assignableTypes = CursorsController.class)
public class CursorsExceptionHandler implements AdviceTrait {

    @ExceptionHandler(UnableProcessException.class)
    public ResponseEntity<Problem> handleUnableProcessException(final UnableProcessException exception,
                                                                final NativeWebRequest request) {
        LOG.error(exception.getMessage(), exception);
        return create(Problem.valueOf(UNPROCESSABLE_ENTITY, exception.getMessage()), request);
    }

    @ExceptionHandler(MethodArgumentNotValidException.class)
    public ResponseEntity<Problem> handleMethodArgumentNotValid(final MethodArgumentNotValidException exception,
                                                                final NativeWebRequest request) {
        LOG.debug(exception.getMessage());
        return create(new ValidationProblem(exception.getBindingResult()), request);
    }

    @ExceptionHandler(NoSuchEventTypeException.class)
    public ResponseEntity<Problem> handleNoSuchEventTypeException(final NoSuchEventTypeException exception,
                                                                  final NativeWebRequest request) {
        LOG.debug(exception.getMessage());
        return create(Problem.valueOf(UNPROCESSABLE_ENTITY, exception.getMessage()), request);
    }

    @ExceptionHandler(InvalidCursorException.class)
    public ResponseEntity<Problem> handleInvalidCursorException(final InvalidCursorException exception,
                                                                final NativeWebRequest request) {
        LOG.debug(exception.getMessage());
        return create(Problem.valueOf(UNPROCESSABLE_ENTITY, exception.getMessage()), request);
    }

    @ExceptionHandler(InvalidStreamIdException.class)
    public ResponseEntity<Problem> handleInvalidStreamIdException(final InvalidStreamIdException exception,
                                                                  final NativeWebRequest request) {
        LOG.debug("Stream id {} is not found: {}", exception.getStreamId(), exception.getMessage());
        return create(Problem.valueOf(UNPROCESSABLE_ENTITY, exception.getMessage()), request);
    }

    @ExceptionHandler(RequestInProgressException.class)
    public ResponseEntity<Problem> handleRequestInProgressException(final RequestInProgressException exception,
                                                                    final NativeWebRequest request) {
        LOG.debug(exception.getMessage());
        return create(Problem.valueOf(CONFLICT, exception.getMessage()), request);
    }

    @ExceptionHandler(CursorsAreEmptyException.class)
    public ResponseEntity<Problem> handleCursorsAreEmptyException(final RuntimeException exception,
                                                                  final NativeWebRequest request) {
        LOG.debug(exception.getMessage());
        return create(Problem.valueOf(UNPROCESSABLE_ENTITY, exception.getMessage()), request);
    }
}
