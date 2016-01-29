package de.zalando.aruha.nakadi.controller;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.databind.node.TreeTraversingParser;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.fasterxml.jackson.datatype.jsonorg.JSONObjectDeserializer;
import com.fasterxml.jackson.datatype.jsonorg.JSONObjectSerializer;
import de.zalando.aruha.nakadi.domain.EventType;
import de.zalando.aruha.nakadi.domain.Problem;
import de.zalando.aruha.nakadi.problem.InvalidJsonProblem;
import de.zalando.aruha.nakadi.repository.EventTypeRepository;
import org.everit.json.schema.Schema;
import org.everit.json.schema.ValidationException;
import org.everit.json.schema.loader.SchemaLoader;
import org.json.JSONObject;
import org.json.JSONTokener;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;
import org.zalando.problem.ProblemModule;

import javax.ws.rs.core.Response;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.Optional;

import static java.util.Optional.empty;
import static org.springframework.http.ResponseEntity.status;

@RestController
@RequestMapping(value = "/event_types")
public class EventTypeController {

    private ObjectMapper jsonMapper;

    private EventTypeRepository repository;

    private Schema schema;

    @Autowired
    public EventTypeController(EventTypeRepository repository, ObjectMapper jsonMapper) {
        this.repository = repository;
        this.jsonMapper = jsonMapper;

        setupJsonMapper();
        loadEventTypeJsonSchema();
    }

    @RequestMapping(method = RequestMethod.POST)
    public ResponseEntity<?> createEventType(@RequestBody final String requestBodyString) throws Exception {
        try {
            schema.validate(new JSONObject(requestBodyString));

            EventType eventType = parseEventType(requestBodyString);

            repository.saveEventType(eventType);

            return status(HttpStatus.CREATED).build();
        } catch (ValidationException e) {
            InvalidJsonProblem problem = new InvalidJsonProblem("Invalid EventType object", e.getMessage());

            return status(422).body(jsonMapper.writer().writeValueAsString(problem));
        } catch (Exception e) {
            return status(500).body(e.getMessage()); // TODO build proper Problem
        }
        // TODO handle duplicated records as 422
    }

    // TODO implement PUT

    // TODO implement GET list

    private EventType parseEventType(String requestBodyString) throws IOException {
        JsonNode requestBodyJsonNode = jsonMapper.reader().readTree(requestBodyString);
        JsonNode eventTypeJsonNode = requestBodyJsonNode.get("event-type");
        JsonParser parser = new TreeTraversingParser(eventTypeJsonNode);

        return jsonMapper.reader().readValue(parser, EventType.class);
    }

    private void loadEventTypeJsonSchema() {
        InputStream inputStream = getClass().getResourceAsStream("/event_type_request.json");
        JSONObject rawSchema = new JSONObject(new JSONTokener(inputStream));
        schema = SchemaLoader.load(rawSchema);
    }

    private void setupJsonMapper() {
        SimpleModule testModule = new SimpleModule();
        testModule.addSerializer(JSONObject.class, new JSONObjectSerializer());
        testModule.addDeserializer(JSONObject.class, new JSONObjectDeserializer());
        this.jsonMapper.registerModule(testModule);
        this.jsonMapper.registerModule(new Jdk8Module());
        this.jsonMapper.registerModule(new ProblemModule());
    }
}
