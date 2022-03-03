package org.zalando.nakadi.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.validation.Errors;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.context.request.NativeWebRequest;
import org.zalando.nakadi.domain.EventType;
import org.zalando.nakadi.domain.EventTypeBase;
import org.zalando.nakadi.domain.EventTypeSchema;
import org.zalando.nakadi.domain.EventTypeSchemaBase;
import org.zalando.nakadi.domain.PaginationWrapper;
import org.zalando.nakadi.exception.SchemaEvolutionException;
import org.zalando.nakadi.exceptions.runtime.InternalNakadiException;
import org.zalando.nakadi.exceptions.runtime.InvalidLimitException;
import org.zalando.nakadi.exceptions.runtime.InvalidVersionNumberException;
import org.zalando.nakadi.exceptions.runtime.NoSuchEventTypeException;
import org.zalando.nakadi.exceptions.runtime.NoSuchSchemaException;
import org.zalando.nakadi.exceptions.runtime.ValidationException;
import org.zalando.nakadi.model.CompatibilityResponse;
import org.zalando.nakadi.service.EventTypeService;
import org.zalando.nakadi.service.SchemaService;
import org.zalando.problem.Problem;
import org.zalando.problem.Status;

import javax.validation.Valid;

import static org.springframework.http.ResponseEntity.status;

@RestController
public class SchemaController {

    private final SchemaService schemaService;
    private final EventTypeService eventTypeService;

    @Autowired
    public SchemaController(final SchemaService schemaService, final EventTypeService eventTypeService) {
        this.schemaService = schemaService;
        this.eventTypeService = eventTypeService;
    }

    @RequestMapping(value = "/event-types/{name}/schemas", method = RequestMethod.POST)
    public ResponseEntity<?> create(@PathVariable("name") final String name,
                                    @Valid @RequestBody final EventTypeSchemaBase schema,
                                    final Errors errors) {
        if (errors.hasErrors()) {
            throw new ValidationException(errors);
        }

        schemaService.addSchema(name, schema);

        return status(HttpStatus.OK).build();
    }

    @RequestMapping(value = "/event-types/{name}/schemas/{version}/compatibility-check", method = RequestMethod.POST)
    public ResponseEntity<?> checkCompatibility(@PathVariable("name") final String name,
                                                @PathVariable("version") final String version,
                                                @Valid @RequestBody final EventTypeSchemaBase schema,
                                                final Errors errors) {
        if (errors.hasErrors()) {
            throw new ValidationException(errors);
        }

        final EventType eventType = eventTypeService.get(name);

        if(!schema.getType().equals(eventType.getSchema().getType())){
            final var detail = String.format("schema type cannot be " +
                            "different: expected `%s`, but was `%s`"
                    , eventType.getSchema().getType(), schema.getType());

            return status(HttpStatus.CONFLICT).
                    body(Problem.valueOf(Status.CONFLICT, detail));
        }

        final var toCompareEventTypeSchema = version.equals("latest")?
                eventType.getSchema(): schemaService.getSchemaVersion(name, version);
        eventType.setSchema(toCompareEventTypeSchema);

        final var newEventTypeBase = new EventTypeBase(eventType);
        newEventTypeBase.setSchema(schema);

        return compatibilityResponse(eventType, newEventTypeBase);
    }

    private ResponseEntity<?> compatibilityResponse(final EventType eventType, final EventTypeBase newEventTypeBase) {
        final var compatibilityMode = newEventTypeBase.getCompatibilityMode();
        final var version = eventType.getSchema().getVersion().toString();
        try {
            schemaService.getValidEvolvedEventType(eventType, newEventTypeBase);
        } catch (SchemaEvolutionException e) {
            return status(HttpStatus.OK).
                    body(new CompatibilityResponse(false, compatibilityMode, e.getMessage(), version));
        }
        return status(HttpStatus.OK).
                body(new CompatibilityResponse(true, compatibilityMode, null, version));
    }

    @RequestMapping(value = "/event-types/{name}/schemas", method = RequestMethod.GET)
    public ResponseEntity<?> getSchemas(
            @PathVariable("name") final String name,
            @RequestParam(value = "offset", required = false, defaultValue = "0") final int offset,
            @RequestParam(value = "limit", required = false, defaultValue = "20") final int limit,
            final NativeWebRequest request)
            throws InvalidLimitException, NoSuchEventTypeException, InternalNakadiException {
        // Ensures that event type exists
        eventTypeService.get(name);

        final PaginationWrapper schemas = schemaService.getSchemas(name, offset, limit);
        return ResponseEntity.status(HttpStatus.OK).body(schemas);
    }

    @RequestMapping("/event-types/{name}/schemas/{version}")
    public ResponseEntity<?> getSchemaVersion(@PathVariable("name") final String name,
                                              @PathVariable("version") final String version,
                                              final NativeWebRequest request)
            throws NoSuchEventTypeException, InternalNakadiException,
            NoSuchSchemaException, InvalidVersionNumberException {
        final EventType eventType = eventTypeService.get(name);
        if (version.equals("latest")) { // latest schema might be cached with the event type
            return ResponseEntity.status(HttpStatus.OK).body(eventType.getSchema());
        }

        final EventTypeSchema result = schemaService.getSchemaVersion(name, version);
        return ResponseEntity.status(HttpStatus.OK).body(result);
    }
}
