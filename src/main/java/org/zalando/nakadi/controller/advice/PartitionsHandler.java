package org.zalando.nakadi.controller.advice;

import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ControllerAdvice;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.context.request.NativeWebRequest;
import org.zalando.nakadi.controller.PartitionsController;
import org.zalando.nakadi.exceptions.runtime.InvalidCursorOperation;
import org.zalando.nakadi.exceptions.runtime.NotFoundException;
import org.zalando.problem.Problem;
import org.zalando.problem.spring.web.advice.AdviceTrait;

import javax.annotation.Priority;

import static org.zalando.problem.Status.NOT_FOUND;
import static org.zalando.problem.Status.UNPROCESSABLE_ENTITY;

@Priority(10)
@ControllerAdvice(assignableTypes = PartitionsController.class)
public class PartitionsHandler implements AdviceTrait {

    static final String INVALID_CURSOR_MESSAGE = "invalid consumed_offset or partition";

    @ExceptionHandler(InvalidCursorOperation.class)
    public ResponseEntity<?> handleInvalidCursorOperation(final InvalidCursorOperation exception,
                                                          final NativeWebRequest request) {
        LOG.debug("User provided invalid cursor for operation. Reason: " + exception.getReason());
        return create(Problem.valueOf(UNPROCESSABLE_ENTITY, INVALID_CURSOR_MESSAGE), request);
    }

    @ExceptionHandler(NotFoundException.class)
    public ResponseEntity<Problem> notFound(final NotFoundException exception, final NativeWebRequest request) {
        LOG.debug(exception.getMessage());
        return create(Problem.valueOf(NOT_FOUND, exception.getMessage()), request);
    }
}
