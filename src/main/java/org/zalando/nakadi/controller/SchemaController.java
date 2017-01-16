package org.zalando.nakadi.controller;

import org.json.JSONArray;
import org.json.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.context.request.NativeWebRequest;
import org.zalando.nakadi.domain.EventType;
import org.zalando.nakadi.domain.EventTypeSchema;
import org.zalando.nakadi.service.EventTypeService;
import org.zalando.nakadi.service.Result;
import org.zalando.nakadi.service.SchemaService;
import org.zalando.nakadi.validation.schema.SchemaGeneration;
import org.zalando.problem.spring.web.advice.Responses;

@RestController
public class SchemaController {

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
        final Result<EventType> eventTypeResult = eventTypeService.get(name);
        if (!eventTypeResult.isSuccessful()) {
            return Responses.create(eventTypeResult.getProblem(), request);
        }

        final Result result = schemaService.getSchemas(name, offset, limit);
        if (result.isSuccessful()) {
            return ResponseEntity.status(HttpStatus.OK).body(result.getValue());
        }
        return Responses.create(result.getProblem(), request);
    }

    @RequestMapping("/event-types/{name}/schemas/{version}")
    public ResponseEntity<?> getSchemaVersion(@PathVariable("name") final String name,
                                              @PathVariable("version") final String version,
                                              final NativeWebRequest request) {
        if (version.equals("latest")) { // latest schema might be cached with the event type
            final Result<EventType> eventTypeResult = eventTypeService.get(name);
            if (!eventTypeResult.isSuccessful()) {
                return Responses.create(eventTypeResult.getProblem(), request);
            }

            return ResponseEntity.status(HttpStatus.OK).body(eventTypeResult.getValue().getSchema());
        }

        final Result<EventTypeSchema> result = schemaService.getSchemaVersion(name, version);
        if (!result.isSuccessful()) {
            return Responses.create(result.getProblem(), request);
        }

        return ResponseEntity.status(HttpStatus.OK).body(result.getValue());
    }

    @RequestMapping(value = "/generate-schema", method = RequestMethod.POST)
    public ResponseEntity<?> generateSchema(@RequestBody final String eventsArrayAsString) {
        final JSONArray events = new JSONArray(eventsArrayAsString);
        JSONObject schema = new JSONObject();
        final SchemaGeneration schemaGeneration = new SchemaGeneration();

        for (final Object event : events) {
            final JSONObject eventSchema = schemaGeneration.schemaFor(event);
            schema = schemaGeneration.mergeSchema(schema, eventSchema);
        }

        return ResponseEntity.status(HttpStatus.OK).body(schema.toString());
    }
}
