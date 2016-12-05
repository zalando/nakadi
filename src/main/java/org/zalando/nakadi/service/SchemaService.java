package org.zalando.nakadi.service;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.zalando.nakadi.domain.EventTypeSchema;
import org.zalando.nakadi.repository.db.SchemaRepository;
import org.zalando.problem.Problem;

import javax.ws.rs.core.Response;
import java.util.List;
import java.util.regex.Pattern;

@Component
public class SchemaService {

    private static final Pattern PATTERN_SCHEMA = Pattern.compile("/(\\d+\\.\\d+\\.\\d+)|(latest)/i");
    private static final String LATEST = "latest";
    private final SchemaRepository schemaRepository;

    @Autowired
    public SchemaService(final SchemaRepository schemaRepository) {
        this.schemaRepository = schemaRepository;
    }

    public Result<List<EventTypeSchema>> getSchemas(final String name) {
        return Result.ok(schemaRepository.getSchemas(name));
    }

    public Result<EventTypeSchema> getSchema(final String name, final String version) {
        if (!PATTERN_SCHEMA.matcher(version).matches())
            return Result.problem(
                    Problem.valueOf(Response.Status.PRECONDITION_FAILED, "Schema version format is wrong"));

        if (LATEST.equalsIgnoreCase(version))
            return Result.ok(schemaRepository.getLastSchemaByName(name));

        return Result.ok(schemaRepository.getSchemaByName(name, version));
    }

}
