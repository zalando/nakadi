package org.zalando.nakadi.webservice;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.zalando.nakadi.config.JsonConfig;
import org.zalando.nakadi.domain.EventType;
import org.zalando.nakadi.repository.kafka.KafkaTestHelper;
import org.apache.http.HttpStatus;
import org.junit.After;
import org.junit.Test;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;

import java.util.Set;

import static com.jayway.restassured.RestAssured.given;
import static com.jayway.restassured.RestAssured.when;
import static com.jayway.restassured.http.ContentType.JSON;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.zalando.nakadi.utils.TestUtils.buildDefaultEventType;
import static org.zalando.nakadi.utils.TestUtils.resourceAsString;

public class EventTypeAT extends BaseAT {

    private static final String ENDPOINT = "/event-types";
    private static final ObjectMapper MAPPER = (new JsonConfig()).jacksonObjectMapper();

    @Test
    public void whenGETThenListsEventTypes() throws JsonProcessingException {
        final EventType eventType = buildDefaultEventType();
        final String body = MAPPER.writer().writeValueAsString(eventType);

        given().body(body).header("accept", "application/json").contentType(JSON).post(ENDPOINT).then().statusCode(
            HttpStatus.SC_CREATED);

        given().header("accept", "application/json").contentType(JSON).when().get(ENDPOINT).then()
               .statusCode(HttpStatus.SC_OK).body("size()", equalTo(1)).body("name[0]", equalTo(eventType.getName()));
    }

    @Test
    public void whenPOSTValidEventTypeThenOk() throws JsonProcessingException {
        final EventType eventType = buildDefaultEventType();

        final String body = MAPPER.writer().writeValueAsString(eventType);

        given().body(body).header("accept", "application/json").contentType(JSON).when().post(ENDPOINT).then()
               .body(equalTo("")).statusCode(HttpStatus.SC_CREATED);
    }

    @Test
    public void rejectTooLongEventTypeNames() throws Exception {
        final String body = resourceAsString("../domain/event-type.with.too-long-name.json", this.getClass());

        given().body(body).header("accept", "application/json").contentType(JSON).when().post(ENDPOINT).then()
                .body(containsString("the length of the name must be")).statusCode(HttpStatus.SC_UNPROCESSABLE_ENTITY);
    }

    @Test
    public void whenPUTValidEventTypeThenOK() throws JsonProcessingException {
        final EventType eventType = buildDefaultEventType();

        final String body = MAPPER.writer().writeValueAsString(eventType);

        given().body(body).header("accept", "application/json").contentType(JSON).post(ENDPOINT);

        given().body(body).header("accept", "application/json").contentType(JSON).when()
               .put(ENDPOINT + "/" + eventType.getName()).then().body(equalTo("")).statusCode(HttpStatus.SC_OK);
    }

    @Test
    public void whenDELETEEventTypeThenOK() throws JsonProcessingException {

        // ARRANGE //
        final EventType eventType = buildDefaultEventType();
        final String body = MAPPER.writer().writeValueAsString(eventType);

        given().body(body).header("accept", "application/json").contentType(JSON).post(ENDPOINT);

        // ACT //
        when().delete(String.format("%s/%s", ENDPOINT, eventType.getName())).then().statusCode(HttpStatus.SC_OK);

        // ASSERT //
        when().get(String.format("%s/%s", ENDPOINT, eventType.getName())).then().statusCode(HttpStatus.SC_NOT_FOUND);

        final KafkaTestHelper kafkaHelper = new KafkaTestHelper(KAFKA_URL);
        final Set<String> allTopics = kafkaHelper.createConsumer().listTopics().keySet();
        assertThat(allTopics, not(hasItem(eventType.getTopic())));
    }

    @After
    public void tearDown() {
        final DriverManagerDataSource datasource = new DriverManagerDataSource(
                POSTGRES_URL,
                POSTGRES_USER,
                POSTGRES_PWD
        );
        final JdbcTemplate template = new JdbcTemplate(datasource);

        template.execute("DELETE FROM zn_data.event_type_schema");
        template.execute("DELETE FROM zn_data.event_type");
    }
}
