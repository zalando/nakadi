package org.zalando.nakadi.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.zalando.nakadi.domain.EventTypeSchema;
import org.zalando.nakadi.exceptions.runtime.NoSuchSchemaException;
import org.zalando.nakadi.repository.db.SchemaRepository;
import org.zalando.problem.Problem;

import javax.ws.rs.core.Response;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Component
public class SchemaService {

    private static final Pattern VERSION_PATTERN = Pattern.compile("\\d+\\.\\d+\\.\\d+");
    private static final Logger LOG = LoggerFactory.getLogger(SchemaService.class);

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

        return Result.ok(paginationService
                .paginate(offset,  limit, String.format("/event-types/%s/schemas", name),
                        (o, l) -> schemaRepository.getSchemas(name, o, l),
                        () -> schemaRepository.getSchemasCount(name)));
    }

    public Result<EventTypeSchema> getSchemaVersion(final String name, final String version) {
        final Matcher versionMatcher = VERSION_PATTERN.matcher(version);
        if (!versionMatcher.matches()) {
            return Result.problem(Problem.valueOf(Response.Status.BAD_REQUEST, "Invalid version number"));
        }

        try {
            final EventTypeSchema schema = schemaRepository.getSchemaVersion(name, version);
            return Result.ok(schema);
        } catch (final NoSuchSchemaException e) {
            LOG.debug("Could not find EventTypeSchema version: {} for EventType: {}", version, name);
            return Result.problem(Problem.valueOf(Response.Status.NOT_FOUND, e.getMessage()));
        }
    }
}
