package org.zalando.nakadi.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.zalando.nakadi.domain.EventTypeSchema;
import org.zalando.nakadi.domain.PaginationWrapper;
import org.zalando.nakadi.exceptions.runtime.InvalidLimitException;
import org.zalando.nakadi.exceptions.runtime.InvalidVersionNumberException;
import org.zalando.nakadi.exceptions.runtime.NoSuchSchemaException;
import org.zalando.nakadi.repository.db.SchemaRepository;

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

    public PaginationWrapper getSchemas(final String name, final int offset, final int limit)
            throws InvalidLimitException {
        if (limit < 1 || limit > 1000) {
            throw new InvalidLimitException("'limit' parameter sholud have value between 1 and 1000");
        }

        if (offset < 0) {
            throw new InvalidLimitException("'offset' parameter can't be lower than 0");
        }

        return paginationService
                .paginate(offset,  limit, String.format("/event-types/%s/schemas", name),
                        (o, l) -> schemaRepository.getSchemas(name, o, l),
                        () -> schemaRepository.getSchemasCount(name));
    }

    public EventTypeSchema getSchemaVersion(final String name, final String version)
            throws NoSuchSchemaException, InvalidVersionNumberException {
        final Matcher versionMatcher = VERSION_PATTERN.matcher(version);
        if (!versionMatcher.matches()) {
            throw new InvalidVersionNumberException("Invalid version number");
        }
        final EventTypeSchema schema = schemaRepository.getSchemaVersion(name, version);
        return schema;
    }
}
