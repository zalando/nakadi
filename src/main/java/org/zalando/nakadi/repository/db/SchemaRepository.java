package org.zalando.nakadi.repository.db;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;
import org.zalando.nakadi.domain.EventTypeSchema;

import java.util.List;

@Component
public class SchemaRepository extends AbstractDbRepository /*implements PagingAndSortingRepository*/ {

    @Autowired
    public SchemaRepository(final JdbcTemplate jdbcTemplate, final ObjectMapper objectMapper) {
        super(jdbcTemplate, objectMapper);
    }

    public List<EventTypeSchema> getSchemas(final String name) {
        return jdbcTemplate.queryForList(
                "SELECT * FROM zn_data.event_type_schema WHERE name = ?",
                new Object[]{name},
                EventTypeSchema.class);
    }

    public EventTypeSchema getSchemaByName(final String name, final String version) {
        return jdbcTemplate.queryForObject(
                "SELECT * FROM zn_data.event_type_schema WHERE name = ? AND version = ?",
                EventTypeSchema.class,
                new Object[]{name, version});
    }

    public EventTypeSchema getLastSchemaByName(final String name) {
        return jdbcTemplate.queryForObject(
                "SELECT * FROM zn_data.event_type_schema WHERE name = ? ORDER BY version DESC LIMIT 1",
                EventTypeSchema.class,
                new Object[]{name});
    }


}
