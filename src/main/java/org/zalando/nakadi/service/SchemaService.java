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

    private static final Pattern PATTERN_SCHEMA = Pattern.compile("\\d+\\.\\d+\\.\\d+|latest");
    private static final String KEY_LATEST_VERSION = "latest";

    private final SchemaRepository schemaRepository;
    private final PaginationService paginationService;

    @Autowired
    public SchemaService(final SchemaRepository schemaRepository,
                         final PaginationService paginationService) {
        this.schemaRepository = schemaRepository;
        this.paginationService = paginationService;
    }

    public Result<?> getSchemas(final String name, final int offset, final int limit) {
        if (limit < 1 || limit > 1000) {
            return Result.problem(Problem.valueOf(Response.Status.BAD_REQUEST,
                    "'limit' parameter should have value from 1 to 1000"));
        }
        if (offset < 0) {
            return Result.problem(Problem.valueOf(Response.Status.BAD_REQUEST,
                    "'offset' parameter can't be lower than 0"));
        }

        final List<EventTypeSchema> schemas = schemaRepository.getSchemas(name, offset, limit + 1);
        return Result.ok(paginationService
                .paginate(schemas, offset,  limit, "/schemas", () -> schemaRepository.getSchemasCount(name)));
    }

    public Result<?> getSchema(final String name, final String version) {
        if (!PATTERN_SCHEMA.matcher(version).matches())
            return Result.problem(
                    Problem.valueOf(Response.Status.PRECONDITION_FAILED, "Schema version format is wrong"));

        if (KEY_LATEST_VERSION.equalsIgnoreCase(version)) {
            return beautifyResult(schemaRepository.getLatestSchemaByName(name));
        }

        return beautifyResult(schemaRepository.getSchemaByName(name, version));
    }

    private Result<?> beautifyResult(final EventTypeSchema schema) {
        return schema == null ? Result.notFound("Schema is not found") : Result.ok(schema);
    }
}
