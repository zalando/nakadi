package org.zalando.nakadi.webservice;

import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.Resources;
import com.jayway.restassured.RestAssured;
import com.jayway.restassured.response.Header;
import com.jayway.restassured.specification.RequestSpecification;
import org.echocat.jomon.runtime.concurrent.RetryForSpecifiedTimeStrategy;
import org.hamcrest.Matchers;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.zalando.nakadi.domain.StreamMetadata;
import org.zalando.nakadi.domain.Subscription;
import org.zalando.nakadi.domain.SubscriptionBase;
import org.zalando.nakadi.domain.SubscriptionCursor;
import org.zalando.nakadi.utils.RandomSubscriptionBuilder;
import org.zalando.nakadi.webservice.hila.StreamBatch;
import org.zalando.nakadi.webservice.utils.TestStreamingClient;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

import static com.jayway.restassured.http.ContentType.JSON;
import static java.util.stream.IntStream.rangeClosed;
import static org.echocat.jomon.runtime.concurrent.Retryer.executeWithRetry;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.springframework.http.HttpStatus.CREATED;
import static org.springframework.http.HttpStatus.NOT_FOUND;
import static org.springframework.http.HttpStatus.NO_CONTENT;
import static org.springframework.http.HttpStatus.OK;
import static org.zalando.nakadi.domain.SubscriptionBase.InitialPosition.BEGIN;
import static org.zalando.nakadi.utils.TestUtils.randomTextString;
import static org.zalando.nakadi.utils.TestUtils.randomValidEventTypeName;
import static org.zalando.nakadi.utils.TestUtils.waitFor;
import static org.zalando.nakadi.webservice.hila.StreamBatch.MatcherIgnoringToken.equalToBatchIgnoringToken;
import static org.zalando.nakadi.webservice.utils.NakadiTestUtils.commitCursors;
import static org.zalando.nakadi.webservice.utils.NakadiTestUtils.createSubscription;

public class UserJourneyAT extends RealEnvironmentAT {

    private static final String EVENT1 = "{\"foo\":\"" + randomTextString() + "\"}";
    private static final String EVENT2 = "{\"foo\":\"" + randomTextString() + "\"}";

    private String eventTypeName;

    private String eventTypeBody;
    private String eventTypeBodyUpdate;

    @Before
    public void before() throws IOException {
        eventTypeName = randomValidEventTypeName();
        eventTypeBody = getEventTypeJsonFromFile("sample-event-type.json", eventTypeName, owningApp);
        eventTypeBodyUpdate = getEventTypeJsonFromFile("sample-event-type-update.json", eventTypeName, owningApp);
    }

    @SuppressWarnings("unchecked")
    @Test(timeout = 15000)
    public void userJourneyM1() throws InterruptedException, IOException {
        // create event-type
        createEventType();

        // get event type
        jsonRequestSpec()
                .when()
                .get("/event-types/" + eventTypeName)
                .then()
                .statusCode(OK.value())
                .body("name", equalTo(eventTypeName))
                .body("owning_application", equalTo(owningApp))
                .body("category", equalTo("undefined"))
                .body("schema.type", equalTo("json_schema"))
                .body("schema.schema", equalTo("{\"type\": \"object\", \"properties\": " +
                        "{\"foo\": {\"type\": \"string\"}}, \"required\": [\"foo\"]}"));

        // list event types
        jsonRequestSpec()
                .when()
                .get("/event-types")
                .then()
                .statusCode(OK.value())
                .body("size()", Matchers.greaterThan(0))
                .body("name[0]", notNullValue())
                .body("owning_application[0]", notNullValue())
                .body("category[0]", notNullValue())
                .body("schema.type[0]", notNullValue())
                .body("schema.schema[0]", notNullValue());

        // update event-type
        jsonRequestSpec()
                .body(eventTypeBodyUpdate)
                .when()
                .put("/event-types/" + eventTypeName)
                .then()
                .statusCode(OK.value());

        // Updates should eventually cause a cache invalidation, so we must retry
        executeWithRetry(() -> {
                    // get event type to check that update is done
                    jsonRequestSpec()
                            .when()
                            .get("/event-types/" + eventTypeName)
                            .then()
                            .statusCode(OK.value())
                            .body("options.retention_time", equalTo(86400000));
                },
                new RetryForSpecifiedTimeStrategy<Void>(5000)
                        .withExceptionsThatForceRetry(AssertionError.class)
                        .withWaitBetweenEachTry(500));

        // push two events to event-type
        postEvents(EVENT1, EVENT2);

        // get offsets for partition
        jsonRequestSpec()
                .when()
                .get("/event-types/" + eventTypeName + "/partitions/0")
                .then()
                .statusCode(OK.value())
                .body("partition", equalTo("0"))
                .body("oldest_available_offset", equalTo("0"))
                .body("newest_available_offset", equalTo("1"));

        // get offsets for all partitions
        jsonRequestSpec()
                .when()
                .get("/event-types/" + eventTypeName + "/partitions")
                .then()
                .statusCode(OK.value())
                .body("size()", equalTo(1)).body("partition[0]", notNullValue())
                .body("oldest_available_offset[0]", notNullValue())
                .body("newest_available_offset[0]", notNullValue());

        // read events
        requestSpec()
                .header(new Header("X-nakadi-cursors", "[{\"partition\": \"0\", \"offset\": \"BEGIN\"}]"))
                .param("batch_limit", "2")
                .param("stream_limit", "2")
                .when()
                .get("/event-types/" + eventTypeName + "/events")
                .then()
                .statusCode(OK.value())
                .body(equalTo("{\"cursor\":{\"partition\":\"0\",\"offset\":\"1\"},\"events\":" + "[" + EVENT1 + ","
                        + EVENT2 + "]}\n"));

        // delete event type
        jsonRequestSpec()
                .when()
                .delete("/event-types/" + eventTypeName)
                .then()
                .statusCode(OK.value());

        // check that it was removed
        jsonRequestSpec()
                .when()
                .get("/event-types/" + eventTypeName)
                .then()
                .statusCode(NOT_FOUND.value());
    }

    @Test(timeout = 15000)
    public void userJourneyHila() throws InterruptedException, IOException {
        // create event-type and push some events
        createEventType();
        postEvents(rangeClosed(0, 3)
                .boxed()
                .map(x -> "{\"foo\":\"bar" + x + "\"}")
                .collect(Collectors.toList())
                .toArray(new String[4]));

        // create subscription
        final SubscriptionBase subscriptionToCreate = RandomSubscriptionBuilder.builder()
                .withOwningApplication("stups_aruha-test-end2end-nakadi")
                .withEventType(eventTypeName)
                .withStartFrom(BEGIN)
                .buildSubscriptionBase();
        final Subscription subscription = createSubscription(jsonRequestSpec(), subscriptionToCreate);

        // list subscriptions
        jsonRequestSpec()
                .param("event_type", eventTypeName)
                .get("/subscriptions")
                .then()
                .statusCode(OK.value())
                .body("items.size()", equalTo(1))
                .body("items[0].id", equalTo(subscription.getId()));

        // create client and wait till we receive all events
        final TestStreamingClient client = new TestStreamingClient(
                RestAssured.baseURI + ":" + RestAssured.port, subscription.getId(), "", oauthToken).start();
        waitFor(() -> assertThat(client.getBatches(), hasSize(4)));
        final List<StreamBatch> batches = client.getBatches();

        // validate the content of events
        for (int i = 0; i < batches.size(); i++) {

            final SubscriptionCursor cursor = new SubscriptionCursor("0", String.valueOf(i), eventTypeName, "");
            final StreamBatch expectedBatch = new StreamBatch(cursor,
                    ImmutableList.of(ImmutableMap.of("foo", "bar" + i)),
                    i == 0 ? new StreamMetadata("Stream started") : null);

            final StreamBatch batch = batches.get(i);
            assertThat(batch, equalToBatchIgnoringToken(expectedBatch));
        }

        // as we didn't commit, there should be still 4 unconsumed events
        jsonRequestSpec()
                .get("/subscriptions/{sid}/stats", subscription.getId())
                .then()
                .statusCode(OK.value())
                .body("items[0].partitions[0].unconsumed_events", equalTo(4));

        // commit cursor of latest event
        final StreamBatch lastBatch = batches.get(batches.size() - 1);
        final int commitCode = commitCursors(jsonRequestSpec(), subscription.getId(),
                ImmutableList.of(lastBatch.getCursor()), client.getSessionId());
        assertThat(commitCode, equalTo(NO_CONTENT.value()));

        // now there should be 0 unconsumed events
        jsonRequestSpec()
                .get("/subscriptions/{sid}/stats", subscription.getId())
                .then()
                .statusCode(OK.value())
                .body("items[0].partitions[0].unconsumed_events", equalTo(0));

        // get cursors
        jsonRequestSpec()
                .get("/subscriptions/{sid}/cursors", subscription.getId())
                .then()
                .statusCode(OK.value())
                .body("items[0].partition", equalTo("0"))
                .body("items[0].offset", equalTo("3"));

        // delete subscription
        jsonRequestSpec()
                .delete("/subscriptions/{sid}", subscription.getId())
                .then()
                .statusCode(NO_CONTENT.value());
    }

    @After
    public void after() {
        jsonRequestSpec().delete("/event-types/" + eventTypeName);
    }

    private void createEventType() {
        jsonRequestSpec()
                .body(eventTypeBody)
                .when()
                .post("/event-types")
                .then()
                .statusCode(CREATED.value());
    }

    private void postEvents(final String... events) {
        final String batch = "[" + String.join(",", events) + "]";
        jsonRequestSpec()
                .body(batch)
                .when()
                .post("/event-types/" + eventTypeName + "/events")
                .then()
                .statusCode(OK.value());
    }

    private RequestSpecification jsonRequestSpec() {
        return requestSpec()
                .header("accept", "application/json")
                .contentType(JSON);
    }

    public static String getEventTypeJsonFromFile(final String resourceName, final String eventTypeName,
                                                  final String owningApp)
            throws IOException {
        final String json = Resources.toString(Resources.getResource(resourceName), Charsets.UTF_8);
        return json
                .replace("NAME_PLACEHOLDER", eventTypeName)
                .replace("OWNING_APP_PLACEHOLDER", owningApp);
    }

}
