package org.zalando.nakadi.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.context.request.NativeWebRequest;
import org.zalando.nakadi.domain.EventTypeSchema;
import org.zalando.nakadi.service.Result;
import org.zalando.nakadi.service.SchemaService;
import org.zalando.problem.spring.web.advice.Responses;

import java.util.List;

@RestController
public class SchemaController {

    private final SchemaService schemaService;

    @Autowired
    public SchemaController(final SchemaService schemaService) {
        this.schemaService = schemaService;
    }

    @RequestMapping("/event-types/{name}/schemas")
    public ResponseEntity<?> getSchemas(
            @PathVariable("name") final String name,
            final NativeWebRequest request) {
        final Result<List<EventTypeSchema>> result = schemaService.getSchemas(name);
        if (result.isSuccessful())
            return ResponseEntity.status(HttpStatus.OK).body(result.getValue());
        return Responses.create(result.getProblem(), request);
    }

    @RequestMapping(value = "/event-types/{name}/schemas/{version}")
    public ResponseEntity<?> getSchemaVersion(
            @PathVariable("name") final String name,
            @PathVariable("version") final String version,
            final NativeWebRequest request) {
        final Result<?> result = schemaService.getSchema(name, version);
        if (result.isSuccessful())
            return ResponseEntity.status(HttpStatus.OK).body(result.getValue());
        return Responses.create(result.getProblem(), request);
    }

}
