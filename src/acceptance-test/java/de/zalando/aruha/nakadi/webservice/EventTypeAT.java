package de.zalando.aruha.nakadi.webservice;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.PropertyNamingStrategy;
import de.zalando.aruha.nakadi.config.NakadiConfig;
import de.zalando.aruha.nakadi.domain.EventType;
import de.zalando.aruha.nakadi.domain.EventTypeSchema;
import org.apache.http.HttpStatus;
import org.json.JSONObject;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static com.jayway.restassured.RestAssured.given;
import static com.jayway.restassured.http.ContentType.JSON;
import static org.hamcrest.core.IsEqual.equalTo;


public class EventTypeAT extends BaseAT {

    static private final String ENDPOINT = "/event_types";

    @Test
    public void whenPOSTValidEventTypeThenOk() throws JsonProcessingException {
        String body = buildEventTypeBody("event-name");

        given().
                body(body).
                when().
                post(ENDPOINT).
                then().
                body(equalTo("")).
                statusCode(HttpStatus.SC_CREATED);
    }

    @Test
    public void whenPOSTInvalidEventTypeThenUnprocessableEntity() throws JsonProcessingException {
        String body = buildEventTypeBody(null);

        given().
                body(body).
                header("accept", "application/json").
                when().
                post(ENDPOINT).
                then().
                statusCode(HttpStatus.SC_UNPROCESSABLE_ENTITY).
                contentType(JSON).
                body("detail", equalTo("#/event-type/name: expected type: String, found: Null")).
                body("title", equalTo("Invalid EventType object")).
                body("status", equalTo(422)).
                body("type", equalTo("https://httpstatuses.com/422"));
    }

    private String buildEventTypeBody(String name) throws JsonProcessingException {
        ObjectMapper mapper = (new NakadiConfig()).jacksonObjectMapper();

        final EventTypeSchema schema = new EventTypeSchema();
        final EventType eventType = new EventType();

        schema.setSchema(new JSONObject("{ \"price\": 1000 }"));
        schema.setType(EventTypeSchema.Type.JSON_SCHEMA);

        eventType.setName(name);
        eventType.setTopic(name + "-topic");
        eventType.setEventTypeSchema(schema);

        Map body = new HashMap<String, Object>();
        body.put("event-type", eventType);

        return mapper.writer().writeValueAsString(body);
    }
}
