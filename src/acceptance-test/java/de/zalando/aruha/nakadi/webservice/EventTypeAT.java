package de.zalando.aruha.nakadi.webservice;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import de.zalando.aruha.nakadi.config.JsonConfig;
import de.zalando.aruha.nakadi.domain.EventCategory;
import de.zalando.aruha.nakadi.domain.EventType;
import de.zalando.aruha.nakadi.domain.EventTypeSchema;
import de.zalando.aruha.nakadi.utils.TestUtils;
import org.apache.http.HttpStatus;
import org.junit.After;
import org.junit.Test;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;

import static com.jayway.restassured.RestAssured.given;
import static com.jayway.restassured.http.ContentType.JSON;
import static org.hamcrest.core.IsEqual.equalTo;


public class EventTypeAT extends BaseAT {

    static private final String ENDPOINT = "/event-types";
    private final ObjectMapper mapper = (new JsonConfig()).jacksonObjectMapper();

    @Test
    public void whenGETThenListsEventTypes() throws JsonProcessingException {
        EventType eventType = buildEventType();
        String body = mapper.writer().writeValueAsString(eventType);

        given().
                body(body).
                header("accept", "application/json").
                contentType(JSON).
                post(ENDPOINT).
                then().
                statusCode(HttpStatus.SC_CREATED);

        given().
                header("accept", "application/json").
                contentType(JSON).
                when().
                get(ENDPOINT).
                then().
                statusCode(HttpStatus.SC_OK).
                body("size()", equalTo(1)).
                body("name[0]", equalTo(eventType.getName()));
    }

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
        final String name = TestUtils.randomString();

        final EventTypeSchema schema = new EventTypeSchema();
        final EventType eventType = new EventType();

        schema.setSchema("{ \"price\": 1000 }");
        schema.setType(EventTypeSchema.Type.JSON_SCHEMA);

        eventType.setName(name);
        eventType.setCategory(EventCategory.UNDEFINED);
        eventType.setSchema(schema);
        eventType.setOwningApplication("producer-application");

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
