package org.zalando.nakadi.webservice;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.io.Resources;
import com.jayway.restassured.RestAssured;
import com.jayway.restassured.http.ContentType;
import com.jayway.restassured.response.Header;
import com.jayway.restassured.specification.RequestSpecification;
import org.echocat.jomon.runtime.concurrent.RetryForSpecifiedTimeStrategy;
import org.hamcrest.Matchers;
import org.json.JSONObject;
import org.junit.Before;
import org.junit.Test;
import org.zalando.nakadi.config.JsonConfig;
import org.zalando.nakadi.domain.EventType;
import org.zalando.nakadi.domain.StreamMetadata;
import org.zalando.nakadi.domain.Subscription;
import org.zalando.nakadi.domain.SubscriptionBase;
import org.zalando.nakadi.utils.RandomSubscriptionBuilder;
import org.zalando.nakadi.utils.TestUtils;
import org.zalando.nakadi.view.PartitionCountView;
import org.zalando.nakadi.view.SubscriptionCursor;
import org.zalando.nakadi.webservice.hila.StreamBatch;
import org.zalando.nakadi.webservice.utils.TestStreamingClient;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static java.util.stream.LongStream.rangeClosed;
import static org.echocat.jomon.runtime.concurrent.Retryer.executeWithRetry;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;
import static org.springframework.http.HttpStatus.CREATED;
import static org.springframework.http.HttpStatus.NOT_FOUND;
import static org.springframework.http.HttpStatus.NO_CONTENT;
import static org.springframework.http.HttpStatus.OK;
import static org.springframework.http.HttpStatus.UNPROCESSABLE_ENTITY;
import static org.zalando.nakadi.domain.SubscriptionBase.InitialPosition.BEGIN;
import static org.zalando.nakadi.utils.TestUtils.randomDate;
import static org.zalando.nakadi.utils.TestUtils.randomTextString;
import static org.zalando.nakadi.utils.TestUtils.randomUUID;
import static org.zalando.nakadi.utils.TestUtils.randomValidEventTypeName;
import static org.zalando.nakadi.utils.TestUtils.waitFor;
import static org.zalando.nakadi.webservice.hila.StreamBatch.MatcherIgnoringToken.equalToBatchIgnoringToken;
import static org.zalando.nakadi.webservice.utils.NakadiTestUtils.commitCursors;
import static org.zalando.nakadi.webservice.utils.NakadiTestUtils.createSubscription;

public class UserJourneyAT extends RealEnvironmentAT {

    private static final String EVENT1 = "{\"foo\":\"" + randomTextString() + "\"}";
    private static final String EVENT2 = "{\"foo\":\"" + randomTextString() + "\"}";
    private static final ObjectMapper MAPPER = (new JsonConfig()).jacksonObjectMapper();
    private static final String ENDPOINT = "/event-types";

    private String eventTypeName;
    private String eventTypeBody;
    private String eventTypeBodyUpdate;

    private String eventTypeNameBusiness;
    private String eventTypeBodyBusiness;

    public static String getEventTypeJsonFromFile(final String resourceName, final String eventTypeName,
                                                  final String owningApp)
            throws IOException {
        final String json = Resources.toString(Resources.getResource(resourceName), Charsets.UTF_8);
        return json
                .replace("NAME_PLACEHOLDER", eventTypeName)
                .replace("OWNING_APP_PLACEHOLDER", owningApp);
    }

    @Before
    public void before() throws IOException {
        eventTypeName = randomValidEventTypeName();
        eventTypeBody = getEventTypeJsonFromFile("sample-event-type.json", eventTypeName, owningApp);
        eventTypeBodyUpdate = getEventTypeJsonFromFile("sample-event-type-update.json", eventTypeName, owningApp);
        createEventType(eventTypeBody);

        eventTypeNameBusiness = eventTypeName + ".business";
        eventTypeBodyBusiness = getEventTypeJsonFromFile(
                "sample-event-type-business.json", eventTypeNameBusiness, owningApp);
        createEventType(eventTypeBodyBusiness);
    }

    @SuppressWarnings("unchecked")
    @Test(timeout = 15000)
    public void userJourneyM1() throws InterruptedException, IOException {

        // get event type
        jsonRequestSpec()
                .when()
                .get("/event-types/" + eventTypeName)
                .then()
                .statusCode(OK.value())
                .body("name", equalTo(eventTypeName))
                .body("owning_application", equalTo(owningApp))
                .body("category", equalTo("undefined"))
                .body("audience", equalTo("external-public"))
                .body("ordering_key_fields", equalTo(Lists.newArrayList("foo", "bar.baz")))
                .body("cleanup_policy", equalTo("delete"))
                .body("schema.type", equalTo("json_schema"))
                .body("schema.schema", equalTo("{\"type\": \"object\", \"properties\": {\"foo\": " +
                        "{\"type\": \"string\"}, \"bar\": {\"type\": \"object\", \"properties\": " +
                        "{\"baz\": {\"type\": \"string\"}}}}, \"required\": [\"foo\"]}"));

        // list event types
        jsonRequestSpec()
                .when()
                .get("/event-types")
                .then()
                .statusCode(OK.value())
                .body("size()", Matchers.greaterThan(0))
                .body("name[0]", Matchers.notNullValue())
                .body("owning_application[0]", Matchers.notNullValue())
                .body("category[0]", Matchers.notNullValue())
                .body("schema.type[0]", Matchers.notNullValue())
                .body("schema.schema[0]", Matchers.notNullValue());

        final String updateEventTypeBody = getUpdateEventType();

        // update event-type
        jsonRequestSpec()
                .body(updateEventTypeBody)
                .when()
                .put("/event-types/" + eventTypeName)
                .then()
                .body(equalTo(""))
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

        // update schema
        jsonRequestSpec()
                .body("{\"type\": \"json_schema\", \"schema\": \"{\\\"type\\\": \\\"object\\\", \\\"properties\\\": " +
                        "{\\\"foo\\\": {\\\"type\\\": \\\"string\\\"}, \\\"bar\\\": {\\\"type\\\": " +
                        "\\\"object\\\", \\\"properties\\\": {\\\"baz\\\": {\\\"type\\\": \\\"string\\\"}," +
                        "\\\"opt\\\": {\\\"type\\\": \\\"string\\\"}}}}, \\\"required\\\": [\\\"foo\\\"]}\"}")
                .post("/event-types/" + eventTypeName + "/schemas")
                .then()
                .body(equalTo(""))
                .statusCode(OK.value());

        executeWithRetry(() -> {
                    // get event type to check that schema update is done
                    jsonRequestSpec()
                            .when()
                            .get("/event-types/" + eventTypeName)
                            .then()
                            .statusCode(OK.value())
                            .body("schema.version", equalTo("1.1.0"));
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
                .body("oldest_available_offset", equalTo("001-0001-000000000000000000"))
                .body("newest_available_offset", equalTo("001-0001-000000000000000001"));

        // get offsets for all partitions
        jsonRequestSpec()
                .when()
                .get("/event-types/" + eventTypeName + "/partitions")
                .then()
                .statusCode(OK.value())
                .body("size()", equalTo(1)).body("partition[0]", Matchers.notNullValue())
                .body("oldest_available_offset[0]", Matchers.notNullValue())
                .body("newest_available_offset[0]", Matchers.notNullValue());

        // read events
        requestSpec()
                .header(new Header("X-nakadi-cursors", "[{\"partition\": \"0\", \"offset\": \"BEGIN\"}]"))
                .param("batch_limit", "2")
                .param("stream_limit", "2")
                .when()
                .get("/event-types/" + eventTypeName + "/events")
                .then()
                .statusCode(OK.value())
                .body(equalTo("{\"cursor\":{\"partition\":\"0\",\"offset\":\"001-0001-000000000000000001\"}," +
                        "\"events\":[" + EVENT1 + "," + EVENT2 + "]}\n"));

        // get distance between cursors
        jsonRequestSpec()
                .body("[{\"initial_cursor\": {\"partition\": \"0\", \"offset\":\"001-0001-000000000000000000\"}, " +
                        "\"final_cursor\": {\"partition\": \"0\", \"offset\":\"001-0001-000000000000000001\"}}]")
                .when()
                .post("/event-types/" + eventTypeName + "/cursor-distances")
                .then()
                .statusCode(OK.value())
                .body("size()", equalTo(1))
                .body("initial_cursor[0].offset", equalTo("001-0001-000000000000000000"))
                .body("final_cursor[0].offset", equalTo("001-0001-000000000000000001"))
                .body("distance[0]", equalTo(1));

        // navigate between cursors
        jsonRequestSpec()
                .body("[{\"partition\": \"0\", \"offset\":\"001-0001-000000000000000000\", \"shift\": 1}, " +
                        "{\"partition\": \"0\", \"offset\":\"001-0001-000000000000000001\", \"shift\": -1}]")
                .when()
                .post("/event-types/" + eventTypeName + "/shifted-cursors")
                .then()
                .statusCode(OK.value())
                .body("size()", equalTo(2))
                .body("offset[0]", equalTo("001-0001-000000000000000001"))
                .body("offset[1]", equalTo("001-0001-000000000000000000"));

        // query for lag
        jsonRequestSpec()
                .body("[{\"partition\": \"0\", \"offset\":\"001-0001-000000000000000000\"}]")
                .when()
                .post("/event-types/" + eventTypeName + "/cursors-lag")
                .then()
                .statusCode(OK.value())
                .body("size()", equalTo(1))
                .body("newest_available_offset[0]", equalTo("001-0001-000000000000000001"))
                .body("oldest_available_offset[0]", equalTo("001-0001-000000000000000000"))
                .body("unconsumed_events[0]", equalTo(1));
    }

    @Test(timeout = 3000)
    public void testRepartition() throws JsonProcessingException {
        //repartition Event Type
        jsonRequestSpec()
                .body(MAPPER.writeValueAsString(new PartitionCountView(3)))
                .when()
                .put("/event-types/" + eventTypeName + "/partition-count")
                .then()
                .body(equalTo(""))
                .statusCode(NO_CONTENT.value());

        // check number of partitions is 3
        jsonRequestSpec()
                .when()
                .get("/event-types/" + eventTypeName + "/partitions")
                .then()
                .statusCode(OK.value())
                .body("size()", equalTo(3));

        // check idempotency
        jsonRequestSpec()
                .body(MAPPER.writeValueAsString(new PartitionCountView(3)))
                .when()
                .put("/event-types/" + eventTypeName + "/partition-count")
                .then()
                .body(equalTo(""))
                .statusCode(NO_CONTENT.value());

        jsonRequestSpec()
                .when()
                .get("/event-types/" + eventTypeName + "/partitions")
                .then()
                .statusCode(OK.value())
                .body("size()", equalTo(3));
    }

    @Test(timeout = 3000)
    public void testEventTypeDeletion() {
        // The reason for separating this test is https://issues.apache.org/jira/browse/KAFKA-2948
        // If producer was used to publish an event, than it is impossible to delete this event type anymore, cause
        // producer will not clean up metadata cache, trying to log that everything is very bad.
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

    private String getUpdateEventType() throws IOException {
        final EventType retrievedEventType = MAPPER.readValue(jsonRequestSpec()
                        .header("accept", "application/json")
                        .get(ENDPOINT + "/" + eventTypeName)
                        .getBody()
                        .asString(),
                EventType.class);

        final EventType updateEventType = MAPPER.readValue(eventTypeBodyUpdate, EventType.class);
        updateEventType.getSchema().setCreatedAt(retrievedEventType.getSchema().getCreatedAt());
        return MAPPER.writer().writeValueAsString(updateEventType);
    }

    @Test(timeout = 15000)
    public void userJourneyHila() throws InterruptedException, IOException {
        postEvents(rangeClosed(0, 3)
                .mapToObj(x -> "{\"foo\":\"bar" + x + "\"}")
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
        waitFor(() -> assertThat(client.getJsonBatches(), Matchers.hasSize(4)));
        final List<StreamBatch> batches = client.getJsonBatches();

        // validate the content of events
        for (int i = 0; i < batches.size(); i++) {
            final SubscriptionCursor cursor = new SubscriptionCursor("0", TestUtils.toTimelineOffset(i),
                    eventTypeName, "");
            final StreamBatch expectedBatch = new StreamBatch(cursor,
                    ImmutableList.of(ImmutableMap.of("foo", "bar" + i)),
                    i == 0 ? new StreamMetadata("Stream started") : null);

            final StreamBatch batch = batches.get(i);
            assertThat(batch, equalToBatchIgnoringToken(expectedBatch));
        }

        // as we didn't commit, there should be still 4 unconsumed events
        jsonRequestSpec()
                .get("/subscriptions/{sid}/stats?show_time_lag=true", subscription.getId())
                .then()
                .statusCode(OK.value())
                .body("items[0].partitions[0].unconsumed_events", equalTo(4))
                .body("items[0].partitions[0].consumer_lag_seconds", Matchers.greaterThanOrEqualTo(0));

        // commit cursor of latest event
        final StreamBatch lastBatch = batches.get(batches.size() - 1);
        final int commitCode = commitCursors(jsonRequestSpec(), subscription.getId(),
                ImmutableList.of(lastBatch.getCursor()), client.getSessionId());
        assertThat(commitCode, equalTo(NO_CONTENT.value()));

        // now there should be 0 unconsumed events
        jsonRequestSpec()
                .get("/subscriptions/{sid}/stats?show_time_lag=true", subscription.getId())
                .then()
                .statusCode(OK.value())
                .body("items[0].partitions[0].unconsumed_events", equalTo(0))
                .body("items[0].partitions[0].consumer_lag_seconds", equalTo(0));

        // get cursors
        jsonRequestSpec()
                .get("/subscriptions/{sid}/cursors", subscription.getId())
                .then()
                .statusCode(OK.value())
                .body("items[0].partition", equalTo("0"))
                .body("items[0].offset", equalTo("001-0001-000000000000000003"));

        // delete subscription
        jsonRequestSpec()
                .delete("/subscriptions/{sid}", subscription.getId())
                .then()
                .statusCode(NO_CONTENT.value());
    }

    @Test(timeout = 15000)
    public void userJourneyAvroTransition() throws InterruptedException, IOException {

        final EventType eventType = MAPPER.readValue(jsonRequestSpec()
                    .header("accept", "application/json")
                    .get(ENDPOINT + "/" + eventTypeNameBusiness)
                    .getBody()
                    .asString(),
                EventType.class);

        final String validatedWithJsonSchemaVersion = eventType.getSchema().getVersion();

        final String event1 = newEvent().put("foo", randomTextString()).toString();
        final String event2 = newEvent().put("foo", randomTextString()).toString();
        final String eventInvalid = newEvent().put("bar", randomTextString()).toString();

        // push two JSON events to event-type
        postEventsInternal(eventTypeNameBusiness, new String[]{event1, event2});

        // try to push some invalid event
        tryPostInvalidEvents(eventTypeNameBusiness, new String[]{eventInvalid});

        // update schema => change to Avro
        jsonRequestSpec()
                .body("{\"type\": \"avro_schema\", " +
                      "\"schema\": \"{\\\"type\\\": \\\"record\\\", \\\"name\\\": \\\"Foo\\\", " +
                      "\\\"fields\\\": [{\\\"name\\\": \\\"foo\\\", \\\"type\\\": \\\"string\\\"}]}\"}")
                .post("/event-types/" + eventTypeNameBusiness + "/schemas")
                .then()
                .body(equalTo(""))
                .statusCode(OK.value());

        // push two more JSON events to Avro event-type
        postEventsInternal(eventTypeNameBusiness, new String[]{event1, event2});

        // test that JSON validation still works
        tryPostInvalidEvents(eventTypeNameBusiness, new String[]{eventInvalid});

        // create subscription
        final SubscriptionBase subscriptionToCreate = RandomSubscriptionBuilder.builder()
                .withOwningApplication("stups_aruha-test-end2end-nakadi")
                .withEventType(eventTypeNameBusiness)
                .withStartFrom(BEGIN)
                .buildSubscriptionBase();
        final Subscription subscription = createSubscription(jsonRequestSpec(), subscriptionToCreate);

        // create client and wait till we receive 4 events
        final TestStreamingClient client = new TestStreamingClient(
                RestAssured.baseURI + ":" + RestAssured.port, subscription.getId(), "", oauthToken).start();

        waitFor(() -> assertThat(client.getJsonBatches(), Matchers.hasSize(4)));
        final List<StreamBatch> batches = client.getJsonBatches();

        // validate the events metadata
        for (final StreamBatch batch : batches) {
            final Map<String, String> metadata = (Map<String, String>) batch.getEvents().get(0).get("metadata");
            assertThat(metadata.get("version"), equalTo(validatedWithJsonSchemaVersion));
        }

        // delete subscription
        jsonRequestSpec()
                .delete("/subscriptions/{sid}", subscription.getId())
                .then()
                .statusCode(NO_CONTENT.value());
    }

    private void createEventType(final String body) {
        jsonRequestSpec()
                .body(body)
                .when()
                .post("/event-types")
                .then()
                .body(equalTo(""))
                .statusCode(CREATED.value());
    }

    private void postEvents(final String... events) {
        postEventsInternal(eventTypeName, events);
    }

    private void postEventsInternal(final String name, final String[] events) {
        final String batch = "[" + String.join(",", events) + "]";
        jsonRequestSpec()
                .body(batch)
                .when()
                .post("/event-types/" + name + "/events")
                .then()
                .body(equalTo(""))
                .statusCode(OK.value());
    }

    private void tryPostInvalidEvents(final String name, final String[] events) {
        final String batch = "[" + String.join(",", events) + "]";
        jsonRequestSpec()
                .body(batch)
                .when()
                .post("/event-types/" + name + "/events")
                .then()
                .statusCode(UNPROCESSABLE_ENTITY.value());
    }

    private RequestSpecification jsonRequestSpec() {
        return requestSpec()
                .header("accept", "application/json")
                .contentType(ContentType.JSON);
    }

    private JSONObject newEvent() {
        return new JSONObject()
                .put("metadata", new JSONObject()
                         .put("eid", randomUUID())
                         .put("occurred_at", randomDate().toString()));
    }
}
