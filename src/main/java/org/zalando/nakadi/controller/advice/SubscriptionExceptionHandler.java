package org.zalando.nakadi.controller.advice;

import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ControllerAdvice;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.context.request.NativeWebRequest;
import org.zalando.nakadi.controller.SubscriptionController;
import org.zalando.nakadi.exceptions.runtime.ErrorGettingCursorTimeLagException;
import org.zalando.nakadi.exceptions.runtime.InconsistentStateException;
import org.zalando.nakadi.exceptions.runtime.TimeLagStatsTimeoutException;
import org.zalando.problem.Problem;
import org.zalando.problem.spring.web.advice.AdviceTrait;

import javax.annotation.Priority;

import static org.zalando.problem.Status.REQUEST_TIMEOUT;
import static org.zalando.problem.Status.SERVICE_UNAVAILABLE;
import static org.zalando.problem.Status.UNPROCESSABLE_ENTITY;

@Priority(10)
@ControllerAdvice(assignableTypes = SubscriptionController.class)
public class SubscriptionExceptionHandler implements AdviceTrait {

    @ExceptionHandler(ErrorGettingCursorTimeLagException.class)
    public ResponseEntity<Problem> handleErrorGettingCursorTimeLagException(
            final ErrorGettingCursorTimeLagException exception,
            final NativeWebRequest request) {
        LOG.debug(exception.getMessage());
        return create(Problem.valueOf(UNPROCESSABLE_ENTITY, exception.getMessage()), request);
    }

    @ExceptionHandler(InconsistentStateException.class)
    public ResponseEntity<Problem> handleInconsistentStateExcetpion(final InconsistentStateException exception,
                                                                    final NativeWebRequest request) {
        LOG.error(exception.getMessage(), exception);
        return create(Problem.valueOf(SERVICE_UNAVAILABLE, exception.getMessage()), request);
    }

    @ExceptionHandler(TimeLagStatsTimeoutException.class)
    public ResponseEntity<Problem> handleTimeLagStatsTimeoutException(final TimeLagStatsTimeoutException exception,
                                                                      final NativeWebRequest request) {
        LOG.warn(exception.getMessage());
        return create(Problem.valueOf(REQUEST_TIMEOUT, exception.getMessage()), request);
    }
}
