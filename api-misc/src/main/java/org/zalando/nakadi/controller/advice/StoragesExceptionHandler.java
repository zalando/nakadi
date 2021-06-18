package org.zalando.nakadi.controller.advice;

import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ControllerAdvice;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.context.request.NativeWebRequest;
import org.zalando.nakadi.controller.StoragesController;
import org.zalando.nakadi.exceptions.runtime.DuplicatedStorageException;
import org.zalando.nakadi.exceptions.runtime.NoSuchStorageException;
import org.zalando.nakadi.exceptions.runtime.StorageIsUsedException;
import org.zalando.nakadi.exceptions.runtime.UnknownStorageTypeException;
import org.zalando.problem.Problem;
import org.zalando.problem.spring.web.advice.AdviceTrait;

import javax.annotation.Priority;

import static org.zalando.problem.Status.CONFLICT;
import static org.zalando.problem.Status.FORBIDDEN;
import static org.zalando.problem.Status.NOT_FOUND;
import static org.zalando.problem.Status.UNPROCESSABLE_ENTITY;

@Priority(10)
@ControllerAdvice(assignableTypes = StoragesController.class)
public class StoragesExceptionHandler implements AdviceTrait {

    @ExceptionHandler(NoSuchStorageException.class)
    public ResponseEntity<Problem> handleNoSuchStorageException(
            final NoSuchStorageException exception,
            final NativeWebRequest request) {
        AdviceTrait.LOG.debug(exception.getMessage());
        return create(Problem.valueOf(NOT_FOUND, exception.getMessage()), request);
    }

    @ExceptionHandler(StorageIsUsedException.class)
    public ResponseEntity<Problem> handleStorageIsUsedException(
            final StorageIsUsedException exception,
            final NativeWebRequest request) {
        AdviceTrait.LOG.debug(exception.getMessage());
        return create(Problem.valueOf(FORBIDDEN, exception.getMessage()), request);
    }

    @ExceptionHandler(DuplicatedStorageException.class)
    public ResponseEntity<Problem> handleDuplicatedStorageException(
            final DuplicatedStorageException exception,
            final NativeWebRequest request) {
        AdviceTrait.LOG.debug(exception.getMessage());
        return create(Problem.valueOf(CONFLICT, exception.getMessage()), request);
    }

    @ExceptionHandler(UnknownStorageTypeException.class)
    public ResponseEntity<Problem> handleUnknownStorageTypeException(
            final UnknownStorageTypeException exception,
            final NativeWebRequest request) {
        AdviceTrait.LOG.debug(exception.getMessage());
        return create(Problem.valueOf(UNPROCESSABLE_ENTITY, exception.getMessage()), request);
    }
}
