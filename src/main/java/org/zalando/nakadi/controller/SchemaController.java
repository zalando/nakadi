package org.zalando.nakadi.controller;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.context.request.NativeWebRequest;
import org.zalando.nakadi.exceptions.runtime.InvalidLimitException;
import org.zalando.nakadi.exceptions.runtime.InvalidOffsetException;
import org.zalando.nakadi.exceptions.runtime.InvalidSchemaVersionException;
import org.zalando.nakadi.exceptions.runtime.NoSuchEventTypeException;
import org.zalando.nakadi.exceptions.runtime.NoSuchSchemaException;
import org.zalando.nakadi.service.EventTypeService;
import org.zalando.nakadi.service.SchemaService;
import org.zalando.problem.Problem;

import static org.zalando.problem.Status.BAD_REQUEST;
import static org.zalando.problem.Status.NOT_FOUND;

@RestController
public class SchemaController implements NakadiProblemHandling {

    private static final Logger LOG = LoggerFactory.getLogger(SchemaController.class);

    private final SchemaService schemaService;
    private final EventTypeService eventTypeService;

    @Autowired
    public SchemaController(final SchemaService schemaService, final EventTypeService eventTypeService) {
        this.schemaService = schemaService;
        this.eventTypeService = eventTypeService;
    }

    @RequestMapping("/event-types/{name}/schemas")
    public ResponseEntity<?> getSchemas(
            @PathVariable("name") final String name,
            @RequestParam(value = "offset", required = false, defaultValue = "0") final int offset,
            @RequestParam(value = "limit", required = false, defaultValue = "20") final int limit,
            final NativeWebRequest request) {
        if (!eventTypeService.exists(name)) {
            throw new NoSuchEventTypeException(String.format("EventType \"%s\" does not exist.", name));
        }
        return ResponseEntity.status(HttpStatus.OK).body(schemaService.getSchemas(name, offset, limit));
    }

    @RequestMapping("/event-types/{name}/schemas/{version}")
    public ResponseEntity<?> getSchemaVersion(@PathVariable("name") final String name,
                                              @PathVariable("version") final String version,
                                              final NativeWebRequest request) {
        if (version.equals("latest")) { // latest schema might be cached with the event type
            return ResponseEntity.status(HttpStatus.OK).body(eventTypeService.get(name).getSchema());
        }
        return ResponseEntity.status(HttpStatus.OK).body(schemaService.getSchemaVersion(name, version));
    }

    @ExceptionHandler(InvalidLimitException.class)
    public ResponseEntity<Problem> handleInvalidLimitException(
            final InvalidLimitException exception,
            final NativeWebRequest request) {
        LOG.debug(exception.getMessage(), exception);
        return create(Problem.valueOf(BAD_REQUEST, exception.getMessage()), request);
    }

    @ExceptionHandler(InvalidOffsetException.class)
    public ResponseEntity<Problem> handleInvalidOffsetException(
            final InvalidOffsetException exception,
            final NativeWebRequest request) {
        LOG.debug(exception.getMessage(), exception);
        return create(Problem.valueOf(BAD_REQUEST, exception.getMessage()), request);
    }

    @ExceptionHandler(InvalidSchemaVersionException.class)
    public ResponseEntity<Problem> handleInvalidSchemaVersionException(
            final InvalidSchemaVersionException exception,
            final NativeWebRequest request) {
        LOG.debug(exception.getMessage(), exception);
        return create(Problem.valueOf(NOT_FOUND, exception.getMessage()), request);
    }

    @ExceptionHandler(NoSuchEventTypeException.class)
    public ResponseEntity<Problem> handleNoSuchEventTypeException(final NoSuchEventTypeException exception,
                                                                  final NativeWebRequest request) {
        LOG.debug(exception.getMessage(), exception);
        return create(Problem.valueOf(NOT_FOUND, exception.getMessage()), request);
    }

    @ExceptionHandler(NoSuchSchemaException.class)
    public ResponseEntity<Problem> handleNoSuchSchemaException(final NoSuchSchemaException exception,
                                                               final NativeWebRequest request) {
        LOG.debug(exception.getMessage(), exception);
        return create(Problem.valueOf(NOT_FOUND, exception.getMessage()), request);
    }
}
