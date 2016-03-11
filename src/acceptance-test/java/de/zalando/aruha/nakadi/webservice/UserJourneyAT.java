package de.zalando.aruha.nakadi.webservice;

import static org.echocat.jomon.runtime.concurrent.Retryer.executeWithRetry;

import static org.hamcrest.Matchers.notNullValue;

import static org.hamcrest.core.IsEqual.equalTo;

import static org.springframework.http.HttpStatus.CREATED;
import static org.springframework.http.HttpStatus.NOT_FOUND;
import static org.springframework.http.HttpStatus.OK;

import static com.jayway.restassured.http.ContentType.JSON;

import static de.zalando.aruha.nakadi.utils.TestUtils.randomTextString;

import java.io.IOException;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

import org.echocat.jomon.runtime.concurrent.RetryForSpecifiedTimeStrategy;

import org.hamcrest.Matchers;

import org.junit.Before;
import org.junit.Test;

import com.google.common.base.Charsets;
import com.google.common.io.Resources;

import com.jayway.restassured.response.Header;
import com.jayway.restassured.specification.RequestSpecification;

import de.zalando.aruha.nakadi.utils.TestUtils;

public class UserJourneyAT extends RealEnvironmentAT {

    private static final String TEST_EVENT_TYPE = TestUtils.randomValidEventTypeName();

    private static final String EVENT1 = "{\"foo\":\"" + randomTextString() + "\"}";
    private static final String EVENT2 = "{\"foo\":\"" + randomTextString() + "\"}";

    private String eventTypeBody;
    private String eventTypeBodyUpdate;

    @Before
    public void before() throws IOException {
        eventTypeBody = getEventTypeJsonFromFile("sample-event-type.json");
        eventTypeBodyUpdate = getEventTypeJsonFromFile("sample-event-type-update.json");
    }

    @SuppressWarnings("unchecked")
    @Test(timeout = 15000)
    public void userJourneyM1() throws InterruptedException {

        // create event-type
        jsonRequestSpec().body(eventTypeBody).when().post("/event-types").then().statusCode(CREATED.value());

        // get event type
        jsonRequestSpec().when().get("/event-types/" + TEST_EVENT_TYPE).then().statusCode(OK.value()).and()
                         .body("name", equalTo(TEST_EVENT_TYPE)).body("owning_application", equalTo("article-producer"))
                         .body("category", equalTo("undefined")).body("schema.type", equalTo("json_schema")).body(
                             "schema.schema",
                             equalTo(
                                 "{\"type\": \"object\", \"properties\": {\"foo\": {\"type\": \"string\"}}, \"required\": [\"foo\"]}"));

        // list event types
        jsonRequestSpec().when().get("/event-types").then().statusCode(OK.value()).and()
                         .body("size()", Matchers.greaterThan(0)).body("name[0]", notNullValue())
                         .body("owning_application[0]", notNullValue()).body("category[0]", notNullValue())
                         .body("schema.type[0]", notNullValue()).body("schema.schema[0]", notNullValue());

        // update event-type
        jsonRequestSpec().body(eventTypeBodyUpdate).when().put("/event-types/" + TEST_EVENT_TYPE).then().statusCode(OK
                .value());

        // Updates should eventually cause a cache invalidation, so we must retry
        executeWithRetry(() -> {

                // get event type to check that update is done
                jsonRequestSpec().when().get("/event-types/" + TEST_EVENT_TYPE).then().statusCode(OK.value()).and()
                    .body("owning_application", equalTo("my-app"));
            },
            new RetryForSpecifiedTimeStrategy<Void>(5000).withExceptionsThatForceRetry(AssertionError.class)
                .withWaitBetweenEachTry(500));

        // push two events to event-type
        postEvent(EVENT1);
        postEvent(EVENT2);

        // get offsets for partition
        jsonRequestSpec().when().get("/event-types/" + TEST_EVENT_TYPE + "/partitions/0").then().statusCode(OK.value())
                         .and().body("partition", equalTo("0")).body("oldest_available_offset", equalTo("0")).body(
                             "newest_available_offset", equalTo("1"));

        // get offsets for all partitions
        jsonRequestSpec().when().get("/event-types/" + TEST_EVENT_TYPE + "/partitions").then().statusCode(OK.value())
                         .and().body("size()", equalTo(8)).body("partition[0]", notNullValue())
                         .body("oldest_available_offset[0]", notNullValue()).body("newest_available_offset[0]",
                             notNullValue());

        // read events
        requestSpec().header(new Header("X-nakadi-cursors", "[{\"partition\": \"0\", \"offset\": \"BEGIN\"}]"))
                     .param("batch_limit", "2").param("stream_limit", "2").when()
                     .get("/event-types/" + TEST_EVENT_TYPE + "/events").then().statusCode(OK.value()).and().body(
                         equalTo(
                             "{\"cursor\":{\"partition\":\"0\",\"offset\":\"1\"},\"events\":" + "[" + EVENT1 + ","
                                 + EVENT2 + "]}\n"));

        // delete event type
        jsonRequestSpec().when().delete("/event-types/" + TEST_EVENT_TYPE).then().statusCode(OK.value());

        // check that it was removed
        jsonRequestSpec().when().get("/event-types/" + TEST_EVENT_TYPE).then().statusCode(NOT_FOUND.value());
    }

    private void postEvent(final String event) {
        jsonRequestSpec().body(event).when().post("/event-types/" + TEST_EVENT_TYPE + "/events").then().statusCode(
            CREATED.value());
    }

    private RequestSpecification jsonRequestSpec() {
        return requestSpec().header("accept", "application/json").contentType(JSON);
    }

    private String getEventTypeJsonFromFile(final String resourceName) throws IOException {
        final String json = Resources.toString(Resources.getResource(resourceName), Charsets.UTF_8);
        return json.replace("NAME_PLACEHOLDER", TEST_EVENT_TYPE);
    }

    private static String timeString() {
        return DateTimeFormatter.ofPattern("yyyy_MM_dd_HH_mm_ss_SSS").format(LocalDateTime.now());
    }

}
