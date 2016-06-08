package de.zalando.aruha.nakadi.webservice;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableSet;
import com.jayway.restassured.http.ContentType;
import com.jayway.restassured.response.Response;
import de.zalando.aruha.nakadi.config.JsonConfig;
import de.zalando.aruha.nakadi.domain.EventType;
import de.zalando.aruha.nakadi.domain.Subscription;
import de.zalando.aruha.nakadi.utils.JsonTestHelper;
import org.apache.http.HttpStatus;
import org.junit.Test;

import java.io.IOException;

import static com.jayway.restassured.RestAssured.given;
import static com.jayway.restassured.http.ContentType.JSON;
import static de.zalando.aruha.nakadi.utils.TestUtils.buildDefaultEventType;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.isEmptyString;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.core.IsEqual.equalTo;

public class SubscriptionAT extends BaseAT {

    private static final String ENDPOINT = "/subscriptions";
    private static final ObjectMapper mapper = (new JsonConfig()).jacksonObjectMapper();
    private static final JsonTestHelper jsonHelper = new JsonTestHelper(mapper);

    @Test
    public void testSubscriptionCreation() throws IOException {
        // create event type in Nakadi
        final EventType eventType = createEventType();

        // create subscription
        final String subscription = "{\"owning_application\":\"app\",\"event_types\":[\"" + eventType.getName() + "\"]}";
        Response response = given()
                .body(subscription)
                .contentType(JSON)
                .post(ENDPOINT);

        // assert response
        response
                .then()
                .statusCode(HttpStatus.SC_CREATED)
                .contentType(ContentType.JSON)
                .body("owning_application", equalTo("app"))
                .body("event_types", containsInAnyOrder(ImmutableSet.of(eventType.getName()).toArray()))
                .body("consumer_group", not(isEmptyString()))
                .body("id", not(isEmptyString()))
                .body("created_at", not(isEmptyString()))
                .body("start_from", not(isEmptyString()));

        // retrieve subscription object from response
        final Subscription subFirst = mapper.readValue(response.print(), Subscription.class);

        // when we try to create that subscription again - we should get status 200
        // and the subscription that already exists should be returned
        response = given()
                .body(subscription)
                .contentType(JSON)
                .post(ENDPOINT);

        // assert status code
        response
                .then()
                .statusCode(HttpStatus.SC_OK)
                .contentType(ContentType.JSON);

        // check that second time already existing subscription was returned
        final Subscription subSecond = mapper.readValue(response.print(), Subscription.class);
        assertThat(subSecond, equalTo(subFirst));
    }

    private EventType createEventType() throws JsonProcessingException {
        final EventType eventType = buildDefaultEventType();
        given()
                .body(mapper.writeValueAsString(eventType))
                .contentType(JSON)
                .post("/event-types");
        return eventType;
    }

}
