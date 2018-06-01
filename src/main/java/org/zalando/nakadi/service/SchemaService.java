package org.zalando.nakadi.service;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.zalando.nakadi.domain.EventTypeSchema;
import org.zalando.nakadi.domain.PaginationWrapper;
import org.zalando.nakadi.exceptions.runtime.InvalidLimitException;
import org.zalando.nakadi.exceptions.runtime.InvalidOffsetException;
import org.zalando.nakadi.exceptions.runtime.InvalidSchemaVersionException;
import org.zalando.nakadi.repository.db.SchemaRepository;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Component
public class SchemaService {

    private static final Pattern VERSION_PATTERN = Pattern.compile("\\d+\\.\\d+\\.\\d+");

    private final SchemaRepository schemaRepository;
    private final PaginationService paginationService;

    @Autowired
    public SchemaService(final SchemaRepository schemaRepository,
                         final PaginationService paginationService) {
        this.schemaRepository = schemaRepository;
        this.paginationService = paginationService;
    }

    public PaginationWrapper getSchemas(final String name, final int offset, final int limit) {
        if (limit < 1 || limit > 1000) {
            throw new InvalidLimitException("'limit' parameter should have value from 1 to 1000");
        }

        if (offset < 0) {
            throw new InvalidOffsetException("'offset' parameter can't be lower than 0");
        }
        return paginationService
                .paginate(offset,  limit, String.format("/event-types/%s/schemas", name),
                        (o, l) -> schemaRepository.getSchemas(name, o, l),
                        () -> schemaRepository.getSchemasCount(name));
    }

    public EventTypeSchema getSchemaVersion(final String name, final String version) {
        final Matcher versionMatcher = VERSION_PATTERN.matcher(version);
        if (!versionMatcher.matches()) {
            throw new InvalidSchemaVersionException("Invalid version number");
        }
        return schemaRepository.getSchemaVersion(name, version);
    }
}
