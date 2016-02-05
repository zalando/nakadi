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
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.support.rowset.SqlRowSet;
import org.springframework.stereotype.Component;

import javax.validation.constraints.NotNull;
import java.io.IOException;
import java.util.Optional;

@Component
public class EventTypeDbRepository implements EventTypeRepository {

    private final JdbcTemplate jdbcTemplate;

    private ObjectMapper jsonMapper;

    @Autowired
    public EventTypeDbRepository(final JdbcTemplate jdbcTemplate, ObjectMapper objectMapper) {
        this.jdbcTemplate = jdbcTemplate;
        this.jsonMapper = objectMapper;
    }

    @Override
    public void saveEventType(EventType eventType) throws DuplicatedEventTypeNameException, NakadiException {
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
    public EventType findByName(@NotNull String name) throws NakadiException, NoSuchEventTypeException {
        SqlRowSet rs = jdbcTemplate.queryForRowSet("SELECT et_name, et_event_type_object FROM zn_data.event_type");

        if(rs.next()) {
            EventType eventType = null;
            try {
                return jsonMapper.readValue(rs.getString(2), EventType.class);
            } catch (IOException e) {
                throw new NakadiException("Problem deserializing event type", e);
            }
        } else {
            throw new NoSuchEventTypeException("EventType \"" + name + "\" does not exist.");
        }
    }

    @Override
    public void update(EventType eventType) throws NakadiException {
        try {
            jdbcTemplate.update(
                    "UPDATE zn_data.event_type SET et_event_type_object = to_json(?::json) WHERE et_name = ?",
                    jsonMapper.writer().writeValueAsString(eventType),
                    eventType.getName());
        } catch (JsonProcessingException e) {
            throw new NakadiException("Serialization problem during persistence of event type", e);
        }
    }

    // TODO create listing
}
