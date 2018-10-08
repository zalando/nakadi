package org.zalando.nakadi.controller.advice;

import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.annotation.ControllerAdvice;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.context.request.NativeWebRequest;
import org.zalando.nakadi.controller.CursorsController;
import org.zalando.nakadi.exceptions.runtime.InvalidCursorException;
import org.zalando.nakadi.exceptions.runtime.NoSuchEventTypeException;
import org.zalando.nakadi.exceptions.runtime.UnableProcessException;
import org.zalando.nakadi.problem.ValidationProblem;
import org.zalando.problem.Problem;
import org.zalando.problem.spring.web.advice.AdviceTrait;

import javax.annotation.Priority;

import static org.zalando.problem.Status.SERVICE_UNAVAILABLE;
import static org.zalando.problem.Status.UNPROCESSABLE_ENTITY;

@Priority(10)
@ControllerAdvice(assignableTypes = CursorsController.class)
public class CursorsHandler implements AdviceTrait {

    @ExceptionHandler(UnableProcessException.class)
    public ResponseEntity<Problem> handleUnableProcessException(final UnableProcessException exception,
                                                                final NativeWebRequest request) {
        LOG.debug(exception.getMessage(), exception);
        return create(Problem.valueOf(SERVICE_UNAVAILABLE, exception.getMessage()), request);
    }

    @ExceptionHandler(MethodArgumentNotValidException.class)
    public ResponseEntity<Problem> handleMethodArgumentNotValid(final MethodArgumentNotValidException exception,
                                                                final NativeWebRequest request) {
        LOG.debug(exception.getMessage(), exception);
        return create(new ValidationProblem(exception.getBindingResult()), request);
    }

    @ExceptionHandler(NoSuchEventTypeException.class)
    public ResponseEntity<Problem> handleNoSuchEventTypeException(final NoSuchEventTypeException exception,
                                                                  final NativeWebRequest request) {
        return create(Problem.valueOf(UNPROCESSABLE_ENTITY, exception.getMessage()), request);
    }

    @ExceptionHandler(InvalidCursorException.class)
    public ResponseEntity<Problem> handleInvalidCursorException(final InvalidCursorException exception,
                                                                final NativeWebRequest request) {
        return create(Problem.valueOf(UNPROCESSABLE_ENTITY, exception.getMessage()), request);
    }
}
