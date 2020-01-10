package org.zalando.nakadi.controller.advice;

import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ControllerAdvice;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.context.request.NativeWebRequest;
import org.zalando.nakadi.controller.SettingsController;
import org.zalando.nakadi.exceptions.runtime.UnableProcessException;
import org.zalando.nakadi.exceptions.runtime.UnknownOperationException;
import org.zalando.problem.Problem;
import org.zalando.problem.spring.web.advice.AdviceTrait;

import javax.annotation.Priority;

import static org.zalando.problem.Status.SERVICE_UNAVAILABLE;
import static org.zalando.problem.Status.UNPROCESSABLE_ENTITY;

@Priority(10)
@ControllerAdvice(assignableTypes = SettingsController.class)
public class SettingsExceptionHandler implements AdviceTrait {

    @ExceptionHandler(UnknownOperationException.class)
    public ResponseEntity<Problem> handleUnknownOperationException(final RuntimeException exception,
                                                                   final NativeWebRequest request) {
        AdviceTrait.LOG.error(exception.getMessage(), exception);
        return create(Problem.valueOf(SERVICE_UNAVAILABLE, "There was a problem processing your request."), request);
    }

    @ExceptionHandler(UnableProcessException.class)
    public ResponseEntity<Problem> handleUnableProcessException(final RuntimeException exception,
                                                                final NativeWebRequest request) {
        AdviceTrait.LOG.debug(exception.getMessage());
        return create(Problem.valueOf(UNPROCESSABLE_ENTITY, exception.getMessage()), request);
    }
}
