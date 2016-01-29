package de.zalando.aruha.nakadi.webservice;

import com.jayway.restassured.response.Response;
import de.zalando.aruha.nakadi.domain.EventType;
import de.zalando.aruha.nakadi.domain.EventTypeSchema;
import org.apache.http.HttpStatus;
import org.json.JSONObject;
import org.junit.Test;
import org.springframework.http.ResponseEntity;
import org.springframework.web.client.RestTemplate;

import java.util.HashMap;
import java.util.Map;

import static com.jayway.restassured.RestAssured.given;
import static java.text.MessageFormat.format;
import static org.hamcrest.core.IsEqual.equalTo;

public class EventTypeAT extends BaseAT {
    @Test
    public void whenPOSTValidEventTypeThenOk() {
        Map body = buildEventTypeBody("event-name");

        final Response response = given().body(body).post("http://localhost:8080/event_types");

        response.then().body(equalTo(""));
        response.then().statusCode(HttpStatus.SC_CREATED);
    }

    @Test
    public void whenPOSTInvalidEventTypeThenUnprocessableEntity() {
        Map body = buildEventTypeBody(null);

        final Response response = given().body(body).post("http://localhost:8080/event_types");

        response.then().body(equalTo(
                "{\"detail\":\"#/event-type/name: expected type: String, found: Null\",\"title\":\"Invalid EventType object\",\"status\":422,\"type\":\"https://httpstatuses.com/422\"}"));
        response.then().statusCode(HttpStatus.SC_UNPROCESSABLE_ENTITY);
    }

    private Map buildEventTypeBody(String name) {
        final EventTypeSchema schema = new EventTypeSchema();
        final EventType eventType = new EventType();

        schema.setSchema(new JSONObject("{ \"price\": 1000 }"));
        schema.setType(EventTypeSchema.Type.JSON_SCHEMA);

        eventType.setName(name);
        eventType.setEventTypeSchema(schema);

        Map body = new HashMap<String, Object>();
        body.put("event-type", eventType);

        return body;
    }
}
