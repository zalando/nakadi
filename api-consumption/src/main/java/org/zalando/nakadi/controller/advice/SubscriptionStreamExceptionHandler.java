package org.zalando.nakadi.controller.advice;

import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ControllerAdvice;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.context.request.NativeWebRequest;
import org.zalando.nakadi.controller.SubscriptionStreamController;
import org.zalando.nakadi.exceptions.runtime.NoStreamingSlotsAvailable;
import org.zalando.nakadi.exceptions.runtime.WrongStreamParametersException;
import org.zalando.problem.Problem;
import org.zalando.problem.spring.web.advice.AdviceTrait;

import javax.annotation.Priority;

import static org.zalando.problem.Status.CONFLICT;
import static org.zalando.problem.Status.UNPROCESSABLE_ENTITY;

@Priority(10)
@ControllerAdvice(assignableTypes = SubscriptionStreamController.class)
public class SubscriptionStreamExceptionHandler implements AdviceTrait {

    @ExceptionHandler(WrongStreamParametersException.class)
    public ResponseEntity<Problem> handleWrongStreamParametersException(final WrongStreamParametersException exception,
                                                                        final NativeWebRequest request) {
        AdviceTrait.LOG.debug(exception.getMessage());
        return create(Problem.valueOf(UNPROCESSABLE_ENTITY, exception.getMessage()), request);
    }

    @ExceptionHandler(NoStreamingSlotsAvailable.class)
    public ResponseEntity<Problem> handleNoStreamingSlotsAvailable(final NoStreamingSlotsAvailable exception,
                                                                   final NativeWebRequest request) {
        AdviceTrait.LOG.debug(exception.getMessage());
        return create(Problem.valueOf(CONFLICT, exception.getMessage()), request);
    }
}
