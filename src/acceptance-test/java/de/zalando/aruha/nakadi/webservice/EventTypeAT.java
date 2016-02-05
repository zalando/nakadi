package de.zalando.aruha.nakadi.webservice;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import de.zalando.aruha.nakadi.config.NakadiConfig;
import de.zalando.aruha.nakadi.domain.EventType;
import de.zalando.aruha.nakadi.domain.EventTypeSchema;
import org.apache.http.HttpStatus;
import org.junit.After;
import org.junit.Test;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;

import static com.jayway.restassured.RestAssured.given;
import static com.jayway.restassured.http.ContentType.JSON;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.core.IsEqual.equalTo;


public class EventTypeAT extends BaseAT {

    static private final String ENDPOINT = "/event_types";
    private final ObjectMapper mapper = (new NakadiConfig()).jacksonObjectMapper();

    @Test
    public void whenPOSTValidEventTypeThenOk() throws JsonProcessingException {
        EventType eventType = buildEventType();

        String body = mapper.writer().writeValueAsString(eventType);

        given().
                body(body).
                header("accept", "application/json").
                contentType(JSON).
                when().
                post(ENDPOINT).
                then().
                body(equalTo("")).
                statusCode(HttpStatus.SC_CREATED);
    }

    @Test
    public void whenPOSTDuplicatedEventTypeNameThenConflict() throws JsonProcessingException {
        EventType eventType = buildEventType();

        String body = mapper.writer().writeValueAsString(eventType);

        given().
                body(body).
                header("accept", "application/json").
                contentType(JSON).
                post(ENDPOINT);


        given().
                body(body).
                header("accept", "application/json").
                contentType(JSON).
                when().
                post(ENDPOINT).
                then().
                body("detail", equalTo("The name \"event-name\" has already been taken.")).
                body("status", equalTo("CONFLICT")).
                body("title", equalTo("Duplicated event type name")).
                body("type", equalTo("https://httpstatuses.com/409")).
                statusCode(HttpStatus.SC_CONFLICT);
    }



    @Test
    public void whenPOSTInvalidEventTypeThenUnprocessableEntity() throws JsonProcessingException {
        EventType eventType = buildEventType();
        eventType.setName("");
        eventType.setCategory("");
        eventType.getEventTypeSchema().setType(null);
        eventType.getEventTypeSchema().setSchema(null);

        String body = mapper.writer().writeValueAsString(eventType);

        given().
                body(body).
                header("accept", "application/json").
                contentType(JSON).
                when().
                post(ENDPOINT).
                then().
                body("detail", containsString("Field \"name\" may not be empty")).
                body("detail", containsString("Field \"category\" may not be empty")).
                body("detail", containsString("Field \"event_type_schema.type\" may not be null")).
                body("detail", containsString("Field \"event_type_schema.schema\" may not be null")).
                body("title", equalTo("Invalid event type")).
                body("status", equalTo("UNPROCESSABLE_ENTITY")).
                body("type", equalTo("https://httpstatuses.com/422")).
                statusCode(HttpStatus.SC_UNPROCESSABLE_ENTITY);
    }

    @Test
    public void whenPUTValidEventTypeThenOK() throws JsonProcessingException {
        EventType eventType = buildEventType();

        String body = mapper.writer().writeValueAsString(eventType);

        given().
                body(body).
                header("accept", "application/json").
                contentType(JSON).
                post(ENDPOINT);

        given().
                body(body).
                header("accept", "application/json").
                contentType(JSON).
                when().
                put(ENDPOINT + "/" + eventType.getName()).
                then().
                body(equalTo("")).
                statusCode(HttpStatus.SC_OK);
    }

    private EventType buildEventType() throws JsonProcessingException {
        final String name = "event-name";

        final EventTypeSchema schema = new EventTypeSchema();
        final EventType eventType = new EventType();

        schema.setSchema("{ \"price\": 1000 }");
        schema.setType(EventTypeSchema.Type.JSON_SCHEMA);

        eventType.setName(name);
        eventType.setCategory(name + "-category");
        eventType.setEventTypeSchema(schema);

        return eventType;
    }

    @After
    public void tearDown() {
        final String postgresqlUrl = "jdbc:postgresql://localhost:5432/local_nakadi_db";
        final String username = "nakadi_app";
        final String password = "nakadi";

        DriverManagerDataSource datasource = new DriverManagerDataSource(postgresqlUrl, username, password);
        JdbcTemplate template = new JdbcTemplate(datasource);

        template.execute("DELETE FROM zn_data.event_type");
    }
}
