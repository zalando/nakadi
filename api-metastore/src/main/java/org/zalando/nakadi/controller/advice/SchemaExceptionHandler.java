package org.zalando.nakadi.controller.advice;

import org.apache.avro.AvroRuntimeException;
import org.json.JSONException;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ControllerAdvice;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.context.request.NativeWebRequest;
import org.zalando.nakadi.controller.SchemaController;
import org.zalando.nakadi.exceptions.runtime.SchemaEvolutionException;
import org.zalando.nakadi.exceptions.runtime.SchemaValidationException;
import org.zalando.nakadi.exceptions.runtime.NoSuchSchemaException;
import org.zalando.problem.Problem;
import org.zalando.problem.spring.web.advice.AdviceTrait;

import javax.annotation.Priority;

import static org.zalando.problem.Status.NOT_FOUND;
import static org.zalando.problem.Status.UNPROCESSABLE_ENTITY;

@Priority(10)
@ControllerAdvice(assignableTypes = SchemaController.class)
public class SchemaExceptionHandler implements AdviceTrait {

    @ExceptionHandler(NoSuchSchemaException.class)
    public ResponseEntity<Problem> handleNoSuchSchemaException(
            final NoSuchSchemaException exception,
            final NativeWebRequest request) {
        AdviceTrait.LOG.debug(exception.getMessage());
        return create(Problem.valueOf(NOT_FOUND, exception.getMessage()), request);
    }

    @ExceptionHandler(SchemaEvolutionException.class)
    public ResponseEntity<Problem> handleSchemaEvolutionException(
            final SchemaEvolutionException exception,
            final NativeWebRequest request) {
        AdviceTrait.LOG.debug(exception.getMessage());
        return create(Problem.valueOf(UNPROCESSABLE_ENTITY, exception.getMessage()), request);
    }

    @ExceptionHandler({SchemaValidationException.class, AvroRuntimeException.class, JSONException.class})
    public ResponseEntity<Problem> handleSchemaValidationException(
            final SchemaValidationException exception,
            final NativeWebRequest request) {
        AdviceTrait.LOG.debug(exception.getMessage());
        return create(Problem.valueOf(UNPROCESSABLE_ENTITY, exception.getMessage()), request);
    }
}
