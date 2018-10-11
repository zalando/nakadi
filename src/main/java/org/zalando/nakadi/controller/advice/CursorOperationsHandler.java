package org.zalando.nakadi.controller.advice;

import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ControllerAdvice;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.context.request.NativeWebRequest;
import org.zalando.nakadi.controller.CursorOperationsController;
import org.zalando.nakadi.exceptions.runtime.CursorConversionException;
import org.zalando.nakadi.exceptions.runtime.InvalidCursorOperation;
import org.zalando.nakadi.exceptions.runtime.NakadiBaseException;
import org.zalando.problem.Problem;
import org.zalando.problem.spring.web.advice.AdviceTrait;

import javax.annotation.Priority;

import static org.zalando.problem.Status.UNPROCESSABLE_ENTITY;

@Priority(10)
@ControllerAdvice(assignableTypes = CursorOperationsController.class)
public class CursorOperationsHandler implements AdviceTrait {


    @ExceptionHandler(InvalidCursorOperation.class)
    public ResponseEntity<?> handleInvalidCursorOperation(final InvalidCursorOperation exception,
                                                          final NativeWebRequest request) {
        LOG.debug("User provided invalid cursor for operation. Reason: " + exception.getReason(), exception);
        return create(Problem.valueOf(UNPROCESSABLE_ENTITY,
                clientErrorMessage(exception.getReason())), request);
    }

    @ExceptionHandler(CursorConversionException.class)
    public ResponseEntity<Problem> handleCursorConversionException(final CursorConversionException exception,
                                                                   final NativeWebRequest request) {
        LOG.error(exception.getMessage(), exception);
        return create(Problem.valueOf(UNPROCESSABLE_ENTITY, exception.getMessage()), request);
    }

    private String clientErrorMessage(final InvalidCursorOperation.Reason reason) {
        switch (reason) {
            case TIMELINE_NOT_FOUND: return "Timeline not found. It might happen in case the cursor refers to a " +
                    "timeline that has already expired.";
            case PARTITION_NOT_FOUND: return "Partition not found.";
            case CURSOR_FORMAT_EXCEPTION: return "Сursor format is not supported.";
            case CURSORS_WITH_DIFFERENT_PARTITION: return "Cursors with different partition. Pairs of cursors should " +
                    "have matching partitions.";
            default:
                LOG.error("Unexpected invalid cursor operation reason " + reason);
                throw new NakadiBaseException();
        }
    }
}
