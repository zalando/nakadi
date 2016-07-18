package de.zalando.aruha.nakadi.repository.db;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import de.zalando.aruha.nakadi.annotations.DB;
import de.zalando.aruha.nakadi.domain.EventType;
import de.zalando.aruha.nakadi.exceptions.InternalNakadiException;
import de.zalando.aruha.nakadi.exceptions.NoSuchEventTypeException;
import de.zalando.aruha.nakadi.exceptions.DuplicatedEventTypeNameException;
import de.zalando.aruha.nakadi.repository.EventTypeRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.DataAccessException;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

@DB
@Component
public class EventTypeDbRepository extends AbstractDbRepository implements EventTypeRepository {

    @Autowired
    public EventTypeDbRepository(final JdbcTemplate jdbcTemplate, final ObjectMapper objectMapper) {
        super(jdbcTemplate, objectMapper);
    }

    @Override
    public void saveEventType(final EventType eventType) throws InternalNakadiException, DuplicatedEventTypeNameException {
        try {
            jdbcTemplate.update("INSERT INTO zn_data.event_type (et_name, et_topic, et_event_type_object) VALUES (?, ?, ?::jsonb)",
                    eventType.getName(),
                    eventType.getTopic(),
                    jsonMapper.writer().writeValueAsString(eventType));
        } catch (JsonProcessingException e) {
            throw new InternalNakadiException("Serialization problem during persistence of event type", e);
        } catch (DuplicateKeyException e) {
            throw new DuplicatedEventTypeNameException("EventType " + eventType.getName() + " already exists.", e);
        }
    }

    @Override
    public EventType findByName(final String name) throws NoSuchEventTypeException {
        final String sql = "SELECT et_topic, et_event_type_object FROM zn_data.event_type WHERE et_name = ?";

        try {
            return jdbcTemplate.queryForObject(sql, new Object[]{name}, new EventTypeMapper());
        } catch (EmptyResultDataAccessException e) {
            throw new NoSuchEventTypeException("EventType \"" + name + "\" does not exist.", e);
        }
    }

    @Override
    public void update(final EventType eventType) throws InternalNakadiException {
        try {
            jdbcTemplate.update(
                    "UPDATE zn_data.event_type SET et_event_type_object = ?::jsonb WHERE et_name = ?",
                    jsonMapper.writer().writeValueAsString(eventType),
                    eventType.getName());
        } catch (JsonProcessingException e) {
            throw new InternalNakadiException("Serialization problem during persistence of event type \""
                    + eventType.getName() + "\"", e);
        }
    }

    private class EventTypeMapper implements RowMapper<EventType> {
        @Override
        public EventType mapRow(final ResultSet rs, final int rowNum) throws SQLException {
            try {
                final EventType eventType = jsonMapper.readValue(rs.getString("et_event_type_object"), EventType.class);
                eventType.setTopic(rs.getString("et_topic"));
                return eventType;
            } catch (IOException e) {
                throw new SQLException(e);
            }
        }
    }

    @Override
    public List<EventType> list() {
        return jdbcTemplate.query(
                "SELECT et_topic, et_event_type_object FROM zn_data.event_type",
                new EventTypeMapper());
    }

    @Override
    public void removeEventType(final String name) throws NoSuchEventTypeException, InternalNakadiException {
        try {
            final int deletedRows = jdbcTemplate.update("DELETE FROM zn_data.event_type WHERE et_name = ?", name);
            if (deletedRows == 0) {
                throw new NoSuchEventTypeException("EventType " + name + " doesn't exist");
            }
        } catch (DataAccessException e) {
            throw new InternalNakadiException("Error occurred when deleting EventType " + name, e);
        }

    }
}
