package org.zalando.nakadi.controller.advice;

import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ControllerAdvice;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.context.request.NativeWebRequest;
import org.zalando.nakadi.controller.PostSubscriptionController;
import org.zalando.nakadi.exceptions.runtime.NakadiBaseException;
import org.zalando.nakadi.exceptions.runtime.SubscriptionCreationDisabledException;
import org.zalando.nakadi.exceptions.runtime.SubscriptionUpdateConflictException;
import org.zalando.nakadi.exceptions.runtime.TooManyPartitionsException;
import org.zalando.nakadi.exceptions.runtime.UnprocessableSubscriptionException;
import org.zalando.nakadi.exceptions.runtime.WrongInitialCursorsException;
import org.zalando.problem.Problem;
import org.zalando.problem.spring.web.advice.AdviceTrait;

import javax.annotation.Priority;

import static org.zalando.problem.Status.SERVICE_UNAVAILABLE;
import static org.zalando.problem.Status.UNPROCESSABLE_ENTITY;

@Priority(10)
@ControllerAdvice(assignableTypes = PostSubscriptionController.class)
public class PostSubscriptionHandler implements AdviceTrait {

    @ExceptionHandler(SubscriptionUpdateConflictException.class)
    public ResponseEntity<Problem> handleSubscriptionUpdateConflictException(
            final SubscriptionUpdateConflictException exception,
            final NativeWebRequest request) {
        LOG.debug(exception.getMessage());
        return create(Problem.valueOf(UNPROCESSABLE_ENTITY, exception.getMessage()), request);
    }

    @ExceptionHandler(SubscriptionCreationDisabledException.class)
    public ResponseEntity<Problem> handleSubscriptionCreationDisabledException(
            final SubscriptionCreationDisabledException exception,
            final NativeWebRequest request) {
        LOG.warn(exception.getMessage());
        return create(Problem.valueOf(SERVICE_UNAVAILABLE, exception.getMessage()), request);
    }

    @ExceptionHandler({
            WrongInitialCursorsException.class,
            TooManyPartitionsException.class,
            UnprocessableSubscriptionException.class})
    public ResponseEntity<Problem> handleUnprocessableSubscription(final NakadiBaseException exception,
                                                                   final NativeWebRequest request) {
        LOG.debug(exception.getMessage());
        return create(Problem.valueOf(UNPROCESSABLE_ENTITY, exception.getMessage()), request);
    }
}
