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
import org.zalando.nakadi.domain.EventType;
import org.zalando.nakadi.domain.EventTypeSchema;
import org.zalando.nakadi.domain.PaginationWrapper;
import org.zalando.nakadi.exceptions.runtime.InternalNakadiException;
import org.zalando.nakadi.exceptions.runtime.InvalidLimitException;
import org.zalando.nakadi.exceptions.runtime.InvalidVersionNumberException;
import org.zalando.nakadi.exceptions.runtime.NoSuchEventTypeException;
import org.zalando.nakadi.exceptions.runtime.NoSuchSchemaException;
import org.zalando.nakadi.service.EventTypeService;
import org.zalando.nakadi.service.SchemaService;
import org.zalando.problem.Problem;
import org.zalando.problem.spring.web.advice.Responses;

import static javax.ws.rs.core.Response.Status.NOT_FOUND;

@RestController
public class SchemaController {

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
            final NativeWebRequest request)
            throws InvalidLimitException, NoSuchEventTypeException, InternalNakadiException {
        final EventType eventType = eventTypeService.get(name);

        final PaginationWrapper schemas = schemaService.getSchemas(name, offset, limit);
        return ResponseEntity.status(HttpStatus.OK).body(schemas);
    }

    @RequestMapping("/event-types/{name}/schemas/{version}")
    public ResponseEntity<?> getSchemaVersion(@PathVariable("name") final String name,
                                              @PathVariable("version") final String version,
                                              final NativeWebRequest request)
            throws NoSuchEventTypeException, InternalNakadiException,
            NoSuchSchemaException, InvalidVersionNumberException {
        if (version.equals("latest")) { // latest schema might be cached with the event type
            final EventType eventType = eventTypeService.get(name);

            return ResponseEntity.status(HttpStatus.OK).body(eventType.getSchema());
        }

        final EventTypeSchema result = schemaService.getSchemaVersion(name, version);
        return ResponseEntity.status(HttpStatus.OK).body(result);
    }

    @ExceptionHandler(NoSuchSchemaException.class)
    public ResponseEntity<Problem> handleNoSuchSchemaException(final NoSuchSchemaException exception,
                                                               final NativeWebRequest request) {
        LOG.debug(exception.getMessage());
        return Responses.create(NOT_FOUND, exception.getMessage(), request);
    }
}
