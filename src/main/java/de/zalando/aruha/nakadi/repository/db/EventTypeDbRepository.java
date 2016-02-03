package de.zalando.aruha.nakadi.repository.db;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.datatype.jsonorg.JSONObjectSerializer;
import de.zalando.aruha.nakadi.NakadiException;
import de.zalando.aruha.nakadi.domain.EventType;
import de.zalando.aruha.nakadi.repository.DuplicatedEventTypeNameException;
import de.zalando.aruha.nakadi.repository.EventTypeRepository;
import org.json.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

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

    // TODO create update feature

    // TODO create listing
}
