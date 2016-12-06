package org.zalando.nakadi.service;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.zalando.nakadi.domain.EventTypeSchema;
import org.zalando.nakadi.repository.db.SchemaRepository;
import org.zalando.problem.Problem;

import javax.ws.rs.core.Response;
import java.util.List;

@Component
public class SchemaService {

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

}
