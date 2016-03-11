package de.zalando.aruha.nakadi.webservice;

import static javax.ws.rs.core.Response.Status.CONFLICT;

import static org.hamcrest.MatcherAssert.assertThat;

import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.not;

import static org.hamcrest.core.IsEqual.equalTo;

import static com.jayway.restassured.RestAssured.given;
import static com.jayway.restassured.RestAssured.when;
import static com.jayway.restassured.http.ContentType.JSON;

import static de.zalando.aruha.nakadi.utils.TestUtils.buildDefaultEventType;

import java.util.Set;

import org.apache.http.HttpStatus;

import org.junit.After;
import org.junit.Test;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;

import org.zalando.problem.Problem;
import org.zalando.problem.ThrowableProblem;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import de.zalando.aruha.nakadi.config.JsonConfig;
import de.zalando.aruha.nakadi.domain.EventType;
import de.zalando.aruha.nakadi.repository.kafka.KafkaTestHelper;
import de.zalando.aruha.nakadi.utils.JsonTestHelper;

public class EventTypeAT extends BaseAT {

    private static final String ENDPOINT = "/event-types";
    private static final ObjectMapper mapper = (new JsonConfig()).jacksonObjectMapper();
    private static final JsonTestHelper jsonHelper = new JsonTestHelper(mapper);

    @Test
    public void whenGETThenListsEventTypes() throws JsonProcessingException {
        final EventType eventType = buildDefaultEventType();
        final String body = mapper.writer().writeValueAsString(eventType);

        given().body(body).header("accept", "application/json").contentType(JSON).post(ENDPOINT).then().statusCode(
            HttpStatus.SC_CREATED);

        given().header("accept", "application/json").contentType(JSON).when().get(ENDPOINT).then()
               .statusCode(HttpStatus.SC_OK).body("size()", equalTo(1)).body("name[0]", equalTo(eventType.getName()));
    }

    @Test
    public void whenPOSTValidEventTypeThenOk() throws JsonProcessingException {
        final EventType eventType = buildDefaultEventType();

        final String body = mapper.writer().writeValueAsString(eventType);

        given().body(body).header("accept", "application/json").contentType(JSON).when().post(ENDPOINT).then()
               .body(equalTo("")).statusCode(HttpStatus.SC_CREATED);
    }

    @Test
    public void whenPUTValidEventTypeThenOK() throws JsonProcessingException {
        final EventType eventType = buildDefaultEventType();

        final String body = mapper.writer().writeValueAsString(eventType);

        given().body(body).header("accept", "application/json").contentType(JSON).post(ENDPOINT);

        given().body(body).header("accept", "application/json").contentType(JSON).when()
               .put(ENDPOINT + "/" + eventType.getName()).then().body(equalTo("")).statusCode(HttpStatus.SC_OK);
    }

    @Test
    public void whenPOSTEventTypeAndTopicExistsThenConflict() throws JsonProcessingException {

        // ARRANGE //
        final EventType eventType = buildDefaultEventType();
        final String body = mapper.writer().writeValueAsString(eventType);

        final KafkaTestHelper kafkaHelper = new KafkaTestHelper(kafkaUrl);
        kafkaHelper.createTopic(eventType.getName(), zookeeperUrl);

        final ThrowableProblem expectedProblem = Problem.valueOf(CONFLICT,
                "EventType with name " + eventType.getName() + " already exists (or wasn't completely removed yet)");

        // ACT //
        given().body(body).header("accept", "application/json").contentType(JSON).when().post(ENDPOINT)
               // ASSERT //
               .then().body(jsonHelper.matchesObject(expectedProblem)).statusCode(HttpStatus.SC_CONFLICT);
    }

    @Test
    public void whenDELETEEventTypeThenOK() throws JsonProcessingException {

        // ARRANGE //
        final EventType eventType = buildDefaultEventType();
        final String body = mapper.writer().writeValueAsString(eventType);

        given().body(body).header("accept", "application/json").contentType(JSON).post(ENDPOINT);

        // ACT //
        when().delete(String.format("%s/%s", ENDPOINT, eventType.getName())).then().statusCode(HttpStatus.SC_OK);

        // ASSERT //
        when().get(String.format("%s/%s", ENDPOINT, eventType.getName())).then().statusCode(HttpStatus.SC_NOT_FOUND);

        final KafkaTestHelper kafkaHelper = new KafkaTestHelper(kafkaUrl);
        final Set<String> allTopics = kafkaHelper.createConsumer().listTopics().keySet();
        assertThat(allTopics, not(hasItem(eventType.getName())));
    }

    @After
    public void tearDown() {
        final String postgresqlUrl = "jdbc:postgresql://localhost:5432/local_nakadi_db";
        final String username = "nakadi_app";
        final String password = "nakadi";

        final DriverManagerDataSource datasource = new DriverManagerDataSource(postgresqlUrl, username, password);
        final JdbcTemplate template = new JdbcTemplate(datasource);

        template.execute("DELETE FROM zn_data.event_type");
    }
}
