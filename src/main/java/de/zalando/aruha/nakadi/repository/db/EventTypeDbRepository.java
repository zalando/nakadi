package de.zalando.aruha.nakadi.repository.db;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import de.zalando.aruha.nakadi.NakadiException;
import de.zalando.aruha.nakadi.domain.EventType;
import de.zalando.aruha.nakadi.repository.DuplicatedEventTypeNameException;
import de.zalando.aruha.nakadi.repository.EventTypeRepository;
import de.zalando.aruha.nakadi.repository.NoSuchEventTypeException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.support.rowset.SqlRowSet;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

@Component
public class EventTypeDbRepository implements EventTypeRepository {

    private final JdbcTemplate jdbcTemplate;
    private final ObjectMapper jsonMapper;

    @Autowired
    public EventTypeDbRepository(final JdbcTemplate jdbcTemplate, final ObjectMapper objectMapper) {
        this.jdbcTemplate = jdbcTemplate;
        this.jsonMapper = objectMapper;
    }

    @Override
    public void saveEventType(final EventType eventType) throws DuplicatedEventTypeNameException, NakadiException {
        try {
            jdbcTemplate.update("INSERT INTO zn_data.event_type (et_name, et_event_type_object) VALUES (?, to_json(?::json))",
                    eventType.getName(),
                    jsonMapper.writer().writeValueAsString(eventType));
        } catch (JsonProcessingException e) {
            throw new NakadiException("Serialization problem during persistence of event type", e);
        } catch (DuplicateKeyException e) {
            throw new DuplicatedEventTypeNameException(e, eventType.getName());
        }
    }

    @Override
    public EventType findByName(final String name) throws NakadiException, NoSuchEventTypeException {
        final String sql = "SELECT et_event_type_object FROM zn_data.event_type WHERE et_name = ?";

        try {
            return jdbcTemplate.queryForObject(sql, new Object[] { name }, new EventTypeMapper());
        } catch (EmptyResultDataAccessException e) {
            throw new NoSuchEventTypeException("EventType \"" + name + "\" does not exist.");
        }
    }

    @Override
    public void update(final EventType eventType) throws NakadiException {
        try {
            jdbcTemplate.update(
                    "UPDATE zn_data.event_type SET et_event_type_object = to_json(?::json) WHERE et_name = ?",
                    jsonMapper.writer().writeValueAsString(eventType),
                    eventType.getName());
        } catch (JsonProcessingException e) {
            throw new NakadiException("Serialization problem during persistence of event type", e);
        }
    }

    private class EventTypeMapper implements RowMapper<EventType> {
        @Override
        public EventType mapRow(ResultSet rs, int rowNum) throws SQLException {
            try {
                return jsonMapper.readValue(rs.getString("et_event_type_object"), EventType.class);
            } catch (IOException e) {
                throw new SQLException(e);
            }
        }
    }

    @Override
    public List<EventType> list() {
        return jdbcTemplate.query(
                "SELECT et_event_type_object FROM zn_data.event_type",
                new EventTypeMapper());
    }
}
