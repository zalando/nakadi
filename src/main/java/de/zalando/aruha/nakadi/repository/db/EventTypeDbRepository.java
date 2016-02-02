package de.zalando.aruha.nakadi.repository.db;

import com.fasterxml.jackson.core.JsonGenerationException;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationConfig;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.datatype.jsonorg.JSONObjectSerializer;
import de.zalando.aruha.nakadi.domain.EventType;
import de.zalando.aruha.nakadi.repository.EventTypeRepository;
import org.codehaus.jackson.JsonGenerator;
import org.codehaus.jackson.map.SerializerProvider;
import org.codehaus.jackson.map.ser.std.SerializerBase;
import org.json.JSONObject;
import org.postgresql.util.PGobject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.SQLException;

@Component
public class EventTypeDbRepository implements EventTypeRepository {

    private final JdbcTemplate jdbcTemplate;

    private ObjectMapper jsonMapper;

    @Autowired
    public EventTypeDbRepository(final JdbcTemplate jdbcTemplate, ObjectMapper jsonMapper) {
        this.jdbcTemplate = jdbcTemplate;
        this.jsonMapper = jsonMapper;

        setupSerializer();
    }

    @Override
    public void saveEventType(EventType eventType) throws Exception {
        try {
            jdbcTemplate.update("INSERT INTO zn_data.event_type (et_name, et_event_type_object) VALUES (?, to_json(?::json))",
                    eventType.getName(),
                    jsonMapper.writer().writeValueAsString(eventType));
        } catch (JsonProcessingException e) {
            // TODO handle errors properly
            throw new Exception(e.getMessage());
        }
    }

    // TODO create update feature

    // TODO create listing

    private void setupSerializer() {
        SimpleModule testModule = new SimpleModule();
        testModule.addSerializer(JSONObject.class, new JSONObjectSerializer());
        this.jsonMapper.registerModule(testModule);
    }
}
