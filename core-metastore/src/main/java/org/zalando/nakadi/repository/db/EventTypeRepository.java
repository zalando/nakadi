package org.zalando.nakadi.repository.db;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.DataAccessException;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Component;
import org.zalando.nakadi.annotations.DB;
import org.zalando.nakadi.domain.Version;
import org.zalando.nakadi.domain.EventType;
import org.zalando.nakadi.domain.EventTypeBase;
import org.zalando.nakadi.exceptions.runtime.DuplicatedEventTypeNameException;
import org.zalando.nakadi.exceptions.runtime.InternalNakadiException;
import org.zalando.nakadi.exceptions.runtime.NoSuchEventTypeException;
import org.zalando.nakadi.plugin.api.authz.AuthorizationAttribute;

import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;

@DB
@Component
public class EventTypeRepository extends AbstractDbRepository {

    @Autowired
    public EventTypeRepository(final JdbcTemplate jdbcTemplate, final ObjectMapper jsonMapper) {
        super(jdbcTemplate, jsonMapper);
    }

    public EventType saveEventType(final EventTypeBase eventTypeBase) throws InternalNakadiException,
            DuplicatedEventTypeNameException {
        try {
            final DateTime now = new DateTime(DateTimeZone.UTC);
            final var version = getStartVersion();
            final EventType eventType = new EventType(eventTypeBase, version, now, now);
            jdbcTemplate.update(
                    "INSERT INTO zn_data.event_type (et_name, et_event_type_object) VALUES (?, ?::jsonb)",
                    eventTypeBase.getName(),
                    jsonMapper.writer().writeValueAsString(eventType));
            insertEventTypeSchema(eventType);
            return eventType;
        } catch (final JsonProcessingException e) {
            throw new InternalNakadiException("Serialization problem during persistence of event type", e);
        } catch (final DataIntegrityViolationException e) {
            throw new DuplicatedEventTypeNameException("EventType " + eventTypeBase.getName() + " already exists.", e);
        }
    }

    public EventType findByName(final String name) throws NoSuchEventTypeException {
        final String sql = "SELECT et_event_type_object FROM zn_data.event_type WHERE et_name = ?";

        try {
            return jdbcTemplate.queryForObject(sql, new Object[]{name}, new EventTypeMapper());
        } catch (EmptyResultDataAccessException e) {
            throw new NoSuchEventTypeException("EventType \"" + name + "\" does not exist.");
        }
    }

    public void update(final EventType eventType) throws InternalNakadiException {
        try {
            final String sql = "SELECT et_event_type_object -> 'schema' ->> 'version' " +
                    "FROM zn_data.event_type WHERE et_name = ?";
            final String currentVersion = jdbcTemplate.queryForObject(sql, String.class, eventType.getName());
            if (!currentVersion.equals(eventType.getSchema().getVersion().toString())) {
                insertEventTypeSchema(eventType);
            }
            jdbcTemplate.update(
                    "UPDATE zn_data.event_type SET et_event_type_object = ?::jsonb WHERE et_name = ?",
                    jsonMapper.writer().writeValueAsString(eventType),
                    eventType.getName());
        } catch (JsonProcessingException e) {
            throw new InternalNakadiException("Serialization problem during persistence of event type \""
                    + eventType.getName() + "\"", e);
        }
    }

    private void insertEventTypeSchema(final EventType eventType) throws JsonProcessingException {
        jdbcTemplate.update(
                "INSERT INTO zn_data.event_type_schema (ets_event_type_name, ets_schema_object) VALUES (?, ?::jsonb)",
                eventType.getName(),
                jsonMapper.writer().writeValueAsString(eventType.getSchema()));
    }

    private class EventTypeMapper implements RowMapper<EventType> {
        @Override
        public EventType mapRow(final ResultSet rs, final int rowNum) throws SQLException {
            try {
                return jsonMapper.readValue(rs.getString("et_event_type_object"), EventType.class);
            } catch (IOException e) {
                throw new SQLException(e);
            }
        }
    }

    public List<EventType> listEventTypesWithRowLock(final Set<String> eventTypes, final RowLockMode lock) {
        final String whereClause = "WHERE zn_data.event_type.et_name in ( "
                + String.join(",", Collections.nCopies(eventTypes.size(), "?")) + ")";
        return jdbcTemplate.query(
                "SELECT et_event_type_object FROM zn_data.event_type " + whereClause + " FOR " + lock.get(),
                eventTypes.toArray(),
                new EventTypeMapper());
    }

    public List<EventType> list(
            final Optional<AuthorizationAttribute> writer,
            final Optional<String> owningApplication) {
        final List<Object> params = Lists.newArrayList();
        final List<String> clauses = Lists.newArrayList();

        final StringBuilder query = new StringBuilder("SELECT et_event_type_object FROM zn_data.event_type");

        writer.ifPresent(_writer -> {
            query.append(" ,jsonb_to_recordset(et_event_type_object->'authorization'->'writers') " +
                    "AS writers(data_type text, value text) ");
            clauses.add("writers.data_type = ?");
            clauses.add("writers.value = ? ");
            params.add(_writer.getDataType());
            params.add(_writer.getValue());
        });

        owningApplication.ifPresent(owningApp -> {
            clauses.add(" et_event_type_object->>'owning_application' = ? ");
            params.add(owningApp);
        });

        if (!clauses.isEmpty()) {
            query.append(" WHERE ");
            query.append(String.join(" AND ", clauses));
        }

        query.append(" ORDER BY et_name ");

        return jdbcTemplate.query(query.toString(), params.toArray(), new EventTypeMapper());
    }

    public void removeEventType(final String name) throws NoSuchEventTypeException, InternalNakadiException {
        try {
            jdbcTemplate.update("DELETE FROM zn_data.event_type_schema WHERE ets_event_type_name = ?", name);
            final int deletedRows = jdbcTemplate.update("DELETE FROM zn_data.event_type WHERE et_name = ?", name);
            if (deletedRows == 0) {
                throw new NoSuchEventTypeException("EventType " + name + " doesn't exist");
            }
        } catch (DataAccessException e) {
            throw new InternalNakadiException("Error occurred when deleting EventType " + name, e);
        }
    }

    private String getStartVersion() {
        return new Version().toString();
    }

    public enum RowLockMode {
        UPDATE("UPDATE"), KEY_SHARE("KEY SHARE");

        private String value;

        RowLockMode(final String value) {
            this.value = value;
        }

        public String get() {
            return value;
        }
    }
}
