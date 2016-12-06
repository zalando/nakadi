package org.zalando.nakadi.repository.db;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Component;
import org.zalando.nakadi.domain.EventTypeSchema;
import org.zalando.nakadi.exceptions.IllegalVersionNumberException;
import org.zalando.nakadi.exceptions.InternalNakadiException;
import org.zalando.nakadi.exceptions.NoSuchSchemaException;

import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Component
public class SchemaRepository extends AbstractDbRepository {

    @Autowired
    public SchemaRepository(final JdbcTemplate jdbcTemplate, final ObjectMapper objectMapper) {
        super(jdbcTemplate, objectMapper);
    }

    public List<EventTypeSchema> getSchemas(final String name, final int offset, final int limit) {
        return jdbcTemplate.query(
                "SELECT ets_schema_object FROM zn_data.event_type_schema " +
                        "WHERE ets_event_type_name = ? ORDER BY ets_schema_object->>'version' DESC LIMIT ? OFFSET ? ",
                new Object[]{name, limit, offset},
                new SchemaRowMapper());
    }

    public EventTypeSchema getSchemaVersion(final String name, final String version)
            throws NoSuchSchemaException, IllegalVersionNumberException, InternalNakadiException {
        final Pattern versionPattern = Pattern.compile("\\d+\\.\\d+\\.\\d+");
        final Matcher versionMatcher = versionPattern.matcher(version);
        if (!versionMatcher.matches()) {
            throw new IllegalVersionNumberException(version);
        }
        final String sql = "SELECT et_event_type_object -> 'schema' ->> 'schema' FROM zn_data.event_type " +
                "WHERE et_name = ? AND et_event_type_object -> 'schema' ->> 'version' = ?";

        try {
            return jdbcTemplate.queryForObject(sql, new Object[]{name, version}, new EventTypeSchemaMapper());
        } catch (EmptyResultDataAccessException e) {
            throw new NoSuchSchemaException("EventType \"" + name
                    + "\" has no schema with version \"" + version + "\"", e);
        }
    }

    public int getSchemasCount(final String name) {
        return jdbcTemplate.queryForObject(
                "SELECT COUNT(*) FROM zn_data.event_type_schema WHERE ets_event_type_name = ?",
                new Object[]{name},
                Integer.class);
    }

    private final class SchemaRowMapper implements RowMapper<EventTypeSchema> {
        @Override
        public EventTypeSchema mapRow(final ResultSet rs, final int rowNum) throws SQLException {
            try {
                return jsonMapper.readValue(rs.getString("ets_schema_object"), EventTypeSchema.class);
            } catch (IOException e) {
                throw new SQLException(e);
            }
        }
    }

    private class EventTypeSchemaMapper implements RowMapper<EventTypeSchema> {
        @Override
        public EventTypeSchema mapRow(final ResultSet rs, final int rowNum) throws SQLException {
            try {
                final EventTypeSchema eventTypeSchema = jsonMapper.readValue(rs.getString(0), EventTypeSchema.class);
                return eventTypeSchema;
            } catch (IOException e) {
                throw new SQLException(e);
            }
        }
    }

}
