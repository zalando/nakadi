package org.zalando.nakadi.webservice.hila;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.jayway.restassured.response.Response;
import org.apache.curator.framework.CuratorFramework;
import org.apache.http.HttpStatus;
import org.junit.Assert;
import org.junit.Test;
import org.zalando.nakadi.config.JsonConfig;
import org.zalando.nakadi.domain.EventType;
import org.zalando.nakadi.domain.EventTypeBase;
import org.zalando.nakadi.domain.EventTypePartition;
import org.zalando.nakadi.domain.Feature;
import org.zalando.nakadi.domain.ItemsWrapper;
import org.zalando.nakadi.domain.PaginationLinks;
import org.zalando.nakadi.domain.PaginationWrapper;
import org.zalando.nakadi.domain.Subscription;
import org.zalando.nakadi.domain.SubscriptionBase;
import org.zalando.nakadi.domain.SubscriptionEventTypeStats;
import org.zalando.nakadi.utils.JsonTestHelper;
import org.zalando.nakadi.utils.RandomSubscriptionBuilder;
import org.zalando.nakadi.utils.TestUtils;
import org.zalando.nakadi.view.Cursor;
import org.zalando.nakadi.view.SubscriptionCursor;
import org.zalando.nakadi.view.SubscriptionCursorWithoutToken;
import org.zalando.nakadi.webservice.BaseAT;
import org.zalando.nakadi.webservice.utils.NakadiTestUtils;
import org.zalando.nakadi.webservice.utils.TestStreamingClient;
import org.zalando.nakadi.webservice.utils.ZookeeperTestUtils;
import org.zalando.problem.Problem;

import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.jayway.restassured.RestAssured.get;
import static com.jayway.restassured.RestAssured.given;
import static com.jayway.restassured.RestAssured.when;
import static com.jayway.restassured.http.ContentType.JSON;
import static java.text.MessageFormat.format;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.isEmptyString;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.zalando.nakadi.utils.TestUtils.buildDefaultEventType;
import static org.zalando.nakadi.utils.TestUtils.randomTextString;
import static org.zalando.nakadi.utils.TestUtils.randomUUID;
import static org.zalando.nakadi.utils.TestUtils.waitFor;
import static org.zalando.nakadi.webservice.utils.NakadiTestUtils.createBusinessEventTypeWithPartitions;
import static org.zalando.nakadi.webservice.utils.NakadiTestUtils.createSubscription;
import static org.zalando.nakadi.webservice.utils.NakadiTestUtils.createSubscriptionForEventType;
import static org.zalando.nakadi.webservice.utils.NakadiTestUtils.createSubscriptionForEventTypeFromBegin;
import static org.zalando.nakadi.webservice.utils.NakadiTestUtils.publishBusinessEventWithUserDefinedPartition;
import static org.zalando.nakadi.webservice.utils.NakadiTestUtils.publishEvents;
import static org.zalando.nakadi.webservice.utils.TestStreamingClient.SESSION_ID_UNKNOWN;
import static org.zalando.problem.Status.UNPROCESSABLE_ENTITY;

public class SubscriptionAT extends BaseAT {

    static final String SUBSCRIPTIONS_URL = "/subscriptions";
    private static final String SUBSCRIPTION_URL = "/subscriptions/{0}";
    private static final String CURSORS_URL = "/subscriptions/{0}/cursors";

    // see application.yml, acceptanceTest profile
    public static final String DELETABLE_OWNING_APP = "deletable_owning_app";
    public static final String DELETABLE_CONSUMER_GROUP = "deletable_consumer_group";

    private static final ObjectMapper MAPPER = (new JsonConfig()).jacksonObjectMapper();
    private static final JsonTestHelper JSON_HELPER = new JsonTestHelper(MAPPER);
    private static final CuratorFramework CURATOR = ZookeeperTestUtils.createCurator(ZOOKEEPER_URL);

    static EventType createEventType() {
        final EventType eventType = buildDefaultEventType();
        try {
            given()
                    .body(MAPPER.writeValueAsString(eventType))
                    .contentType(JSON)
                    .post("/event-types");
            return eventType;
        } catch (final JsonProcessingException ex) {
            throw new RuntimeException(ex);
        }
    }

    @Test
    public void testSubscriptionBaseOperations() throws IOException {
        // create event type in Nakadi
        final EventType eventType = createEventType();

        // create subscription
        final String subscription = "{\"owning_application\":\"app\",\"event_types\":[\"" + eventType.getName() +
                "\"]}";
        Response response = given()
                .body(subscription)
                .contentType(JSON)
                .post(SUBSCRIPTIONS_URL);

        // assert response
        response
                .then()
                .statusCode(HttpStatus.SC_CREATED)
                .contentType(JSON)
                .body("owning_application", equalTo("app"))
                .body("event_types", containsInAnyOrder(ImmutableSet.of(eventType.getName()).toArray()))
                .body("consumer_group", not(isEmptyString()))
                .body("id", not(isEmptyString()))
                .body("created_at", not(isEmptyString()))
                .body("start_from", not(isEmptyString()));

        // retrieve subscription object from response
        final Subscription subFirst = MAPPER.readValue(response.print(), Subscription.class);
        //check initialization of updated_At
        assertThat(subFirst.getUpdatedAt(), equalTo(subFirst.getCreatedAt()));
        // when we try to create that subscription again - we should get status 200
        // and the subscription that already exists should be returned
        response = given()
                .body(subscription)
                .contentType(JSON)
                .post(SUBSCRIPTIONS_URL);

        // assert status code
        response
                .then()
                .statusCode(HttpStatus.SC_OK)
                .contentType(JSON);

        // check that second time already existing subscription was returned
        final Subscription subSecond = MAPPER.readValue(response.print(), Subscription.class);
        assertThat(subSecond, equalTo(subFirst));

        // check get subscription endpoint
        response = get(format(SUBSCRIPTION_URL, subFirst.getId()));
        response.then().statusCode(HttpStatus.SC_OK).contentType(JSON);
        final Subscription gotSubscription = MAPPER.readValue(response.print(), Subscription.class);
        assertThat(gotSubscription, equalTo(subFirst));

    }

    @Test
    public void testQuerySubscriptionByAuthReturnsOk() throws JsonProcessingException {
        final String serviceName = "stups_test-app" + randomTextString();
        final EventType eventType = createEventType();
        final String authAttr = "{\"data_type\":\"service\",\"value\": \"" + serviceName + "\"}";
        final String subscription = "{\"owning_application\":\"app\",\"event_types\":[\""
                + eventType.getName() + "\"], \"read_from\": \"end\", \"consumer_group\":\"test-%s\"," +
                "\"authorization\": {\"admins\": [" + authAttr + "], \"readers\": [" + authAttr + "]}}";

        final Subscription firstSub = MAPPER.readValue(given()
                .body(String.format(subscription, "first-group"))
                .contentType(JSON)
                .post(SUBSCRIPTIONS_URL).print(), Subscription.class);
        final Subscription secondSub = MAPPER.readValue(given()
                .body(String.format(subscription, "second-group"))
                .contentType(JSON)
                .post(SUBSCRIPTIONS_URL).print(), Subscription.class);

        final PaginationLinks.Link paginationLink = new PaginationLinks.Link(
                String.format("%s?reader=service:%s&offset=2&limit=2", SUBSCRIPTIONS_URL, serviceName));
        final PaginationWrapper<Subscription> expectedList = new PaginationWrapper<>(
                ImmutableList.of(secondSub, firstSub),
                new PaginationLinks(Optional.empty(), Optional.of(paginationLink)));

        given()
                .param("reader", "service:" + serviceName)
                .param("limit", 2)
                .get("/subscriptions")
                .then()
                .statusCode(HttpStatus.SC_OK)
                .body(JSON_HELPER.matchesObject(expectedList));
    }

    @Test
    public void testSubscriptionWithNullAuthorisation() {
        final EventType eventType = createEventType();
        final String subscription = "{\"owning_application\":\"app\",\"event_types\":[\""
                + eventType.getName() + "\"], \"read_from\": \"end\", \"consumer_group\":\"test\"," +
                "\"authorization\": {\"admins\": [], \"readers\": []}}";
        final Response response = given().body(subscription).contentType(JSON).post(SUBSCRIPTIONS_URL);
        response.then()
                .statusCode(HttpStatus.SC_UNPROCESSABLE_ENTITY)
                .contentType(JSON)
                .body("title", equalTo("Unprocessable Entity"));
    }

    @Test
    public void testSubscriptionWithManyEventTypesIsCreated() throws IOException {
        final List<String> eventTypes = IntStream.range(0, 10).mapToObj(i -> createEventType())
                .map(EventTypeBase::getName)
                .collect(Collectors.toList());
        final String subscription = "{\"owning_application\":\"app\",\"event_types\":" +
                "[" + eventTypes.stream().map(et -> "\"" + et + "\"").collect(Collectors.joining(",")) + "]}";
        final Response response = given()
                .body(subscription)
                .contentType(JSON)
                .post(SUBSCRIPTIONS_URL);
        // assert response
        response.then().statusCode(HttpStatus.SC_CREATED).contentType(JSON);
        final Subscription gotSubscription = MAPPER.readValue(response.print(), Subscription.class);
        Assert.assertNotNull(gotSubscription.getId());
    }

    @Test
    public void testSubscriptionWithManyEventTypesIsNotCreated() {
        final List<String> eventTypes = IntStream.range(0, 31).mapToObj(i -> createEventType())
                .map(EventTypeBase::getName)
                .collect(Collectors.toList());
        final String subscription = "{\"owning_application\":\"app\",\"event_types\":" +
                "[" + eventTypes.stream().map(et -> "\"" + et + "\"").collect(Collectors.joining(",")) + "]}";
        final Response response = given()
                .body(subscription)
                .contentType(JSON)
                .post(SUBSCRIPTIONS_URL);
        // assert response
        response
                .then()
                .statusCode(HttpStatus.SC_UNPROCESSABLE_ENTITY)
                .contentType(JSON)
                .body("title", equalTo("Unprocessable Entity"))
                .body("detail", equalTo(
                        "total partition count for subscription is 31, but the maximum partition count is 30"));

    }

    @Test
    public void testListSubscriptions() throws IOException {
        final String etName = createEventType().getName();

        final String filterApp = randomUUID();
        final Subscription sub1 = createSubscription(RandomSubscriptionBuilder.builder()
                .withEventType(etName).withOwningApplication(filterApp).buildSubscriptionBase());
        final Subscription sub2 = createSubscription(RandomSubscriptionBuilder.builder()
                .withEventType(etName).withOwningApplication(filterApp).buildSubscriptionBase());
        createSubscription(RandomSubscriptionBuilder.builder().withEventType(etName).buildSubscriptionBase());

        final PaginationWrapper<Subscription> expectedList = new PaginationWrapper<>(ImmutableList.of(sub2, sub1),
                new PaginationLinks());

        given()
                .param("owning_application", filterApp)
                .get("/subscriptions")
                .then()
                .statusCode(HttpStatus.SC_OK)
                .body(JSON_HELPER.matchesObject(expectedList));
    }

    @Test
    public void testOffsetsCommit() throws Exception {
        // create event type in Nakadi
        final String etName = createEventType().getName();

        final Subscription subscription = createSubscriptionForEventType(etName);

        final TestStreamingClient client = TestStreamingClient
                .create(URL, subscription.getId(), "")
                .start();
        waitFor(() -> assertThat(client.getSessionId(), not(equalTo(SESSION_ID_UNKNOWN))));

        // commit lower offsets and expect 200
        String cursor = "{\"items\":[{\"partition\":\"0\",\"offset\":\"-1\",\"event_type\":\"" + etName +
                "\",\"cursor_token\":\"abc\"}]}";
        commitCursors(subscription, cursor, client.getSessionId())
                .then()
                .statusCode(HttpStatus.SC_OK);

        cursor = "{\"items\":[{\"partition\":\"0\",\"offset\":\"25\",\"event_type\":\"" + etName +
                "\",\"cursor_token\":\"abc\"}]}";
        commitCursors(subscription, cursor, client.getSessionId())
                .then()
                .statusCode(HttpStatus.SC_NO_CONTENT);

        // check that offset is actually committed to Zookeeper
        final String committedOffset = getCommittedOffsetFromZk(etName, subscription, "0");
        assertThat(committedOffset, equalTo(TestUtils.toTimelineOffset(25)));
    }

    @Test
    public void testSubscriptionWithReadFromCursorsWithoutInitialCursors() throws Exception {
        final EventType eventType = createEventType();

        final SubscriptionBase subscriptionBase = RandomSubscriptionBuilder.builder()
                .withEventType(eventType.getName())
                .withStartFrom(SubscriptionBase.InitialPosition.CURSORS)
                .buildSubscriptionBase();

        given()
                .body(JSON_HELPER.asJsonString(subscriptionBase))
                .contentType(JSON)
                .post(SUBSCRIPTIONS_URL)
                .then()
                .statusCode(HttpStatus.SC_UNPROCESSABLE_ENTITY)
                .body("detail", equalTo("initial_cursors should contain cursors for all partitions of subscription"));
    }

    @Test
    public void testSubscriptionWithInitialCursors() throws Exception {
        final EventType et1 = createBusinessEventTypeWithPartitions(2);
        final EventType et2 = createBusinessEventTypeWithPartitions(2);

        // write 10 events to each partition of two event-types
        publishBusinessEventWithUserDefinedPartition(et1.getName(), 10, i -> "dummy", i -> "0");
        publishBusinessEventWithUserDefinedPartition(et1.getName(), 10, i -> "dummy", i -> "1");
        publishBusinessEventWithUserDefinedPartition(et2.getName(), 10, i -> "dummy", i -> "0");
        publishBusinessEventWithUserDefinedPartition(et2.getName(), 10, i -> "dummy", i -> "1");

        // create subscription with initial cursors
        final SubscriptionBase subscriptionBase = RandomSubscriptionBuilder.builder()
                .withEventTypes(ImmutableSet.of(et1.getName(), et2.getName()))
                .withStartFrom(SubscriptionBase.InitialPosition.CURSORS)
                .withInitialCursors(ImmutableList.of(
                        new SubscriptionCursorWithoutToken(et1.getName(), "0", "000000000000000007"),
                        new SubscriptionCursorWithoutToken(et1.getName(), "1", "000000000000000002"),
                        new SubscriptionCursorWithoutToken(et2.getName(), "0", Cursor.BEFORE_OLDEST_OFFSET),
                        new SubscriptionCursorWithoutToken(et2.getName(), "1", "000000000000000009")
                ))
                .buildSubscriptionBase();
        final Subscription subscription = createSubscription(subscriptionBase);

        final TestStreamingClient client = TestStreamingClient
                .create(URL, subscription.getId(), "max_uncommitted_events=100")
                .start();
        waitFor(() -> assertThat(client.getJsonBatches(), hasSize(19))); // we should read 19 events in total
        final List<StreamBatch> batches = client.getJsonBatches();

        // check that first events of each partition have correct offsets
        assertThat(getFirstBatchOffsetFor(batches, new EventTypePartition(et1.getName(), "0")),
                equalTo(Optional.of("001-0001-000000000000000008")));
        assertThat(getFirstBatchOffsetFor(batches, new EventTypePartition(et1.getName(), "1")),
                equalTo(Optional.of("001-0001-000000000000000003")));
        assertThat(getFirstBatchOffsetFor(batches, new EventTypePartition(et2.getName(), "0")),
                equalTo(Optional.of("001-0001-000000000000000000")));
        assertThat(getFirstBatchOffsetFor(batches, new EventTypePartition(et2.getName(), "1")),
                equalTo(Optional.empty()));
    }

    private Optional<String> getFirstBatchOffsetFor(final List<StreamBatch> batches,
                                                    final EventTypePartition etPartition) {
        return batches.stream()
                .filter(b -> etPartition.ownsCursor(b.getCursor()))
                .findFirst()
                .map(b -> b.getCursor().getOffset());
    }

    @Test
    public void testGetSubscriptionCursors() throws IOException, InterruptedException {
        final String etName = createEventType().getName();
        final Subscription subscription = createSubscriptionForEventType(etName);
        final String cursor = "{\"items\":[{\"partition\":\"0\",\"offset\":\"25\",\"event_type\":\"" + etName +
                "\",\"cursor_token\":\"abc\"}]}";

        final TestStreamingClient client = TestStreamingClient
                .create(URL, subscription.getId(), "")
                .start();
        waitFor(() -> assertThat(client.getSessionId(), not(equalTo(SESSION_ID_UNKNOWN))));

        commitCursors(subscription, cursor, client.getSessionId())
                .then()
                .statusCode(HttpStatus.SC_NO_CONTENT);

        final List<SubscriptionCursor> actualCursors = NakadiTestUtils.getSubscriptionCursors(subscription).getItems();
        assertThat(actualCursors, hasSize(1));

        final SubscriptionCursor actualCursor = actualCursors.get(0);
        assertThat(actualCursor.getPartition(), equalTo("0"));
        assertThat(actualCursor.getOffset(), equalTo(TestUtils.toTimelineOffset(25)));
        assertThat(actualCursor.getEventType(), equalTo(etName));
    }

    @Test
    public void testGetSubscriptionCursorsEmpty() throws IOException {
        final String etName = createEventType().getName();
        final Subscription subscription = createSubscriptionForEventType(etName);
        Assert.assertTrue(NakadiTestUtils.getSubscriptionCursors(subscription).getItems().isEmpty());
    }

    @Test
    public void testGetSubscriptionNotFound() throws IOException {
        given()
                .get(format(CURSORS_URL, "UNKNOWN_SUB_ID"))
                .then()
                .statusCode(HttpStatus.SC_NOT_FOUND);
    }

    @Test
    public void testDeleteSubscription() throws Exception {
        final String etName = createEventType().getName();
        final Subscription subscription = createSubscriptionForEventType(etName);

        // try reading from subscription so that the lock is created in ZK
        final TestStreamingClient client = TestStreamingClient
                .create(URL, subscription.getId(), "")
                .start();
        waitFor(() -> assertThat(client.getSessionId(), not(equalTo(SESSION_ID_UNKNOWN))));

        // check that subscription node and lock node were created in ZK
        assertThat(
                CURATOR.checkExists().forPath(format("/nakadi/subscriptions/{0}", subscription.getId())),
                not(nullValue()));

        // delete subscription
        when().delete("/subscriptions/{sid}", subscription.getId())
                .then()
                .statusCode(HttpStatus.SC_NO_CONTENT);

        // check that ZK nodes were removed
        assertThat(
                CURATOR.checkExists().forPath(format("/nakadi/subscriptions/{0}", subscription.getId())),
                nullValue());
        assertThat(
                CURATOR.checkExists().forPath(format("/nakadi/locks/subscription_{0}", subscription.getId())),
                nullValue());
    }

    @Test
    public void testDeleteEventTypeRestriction() throws Exception {
        final String etName = createEventType().getName();
        createSubscriptionForEventType(etName);

        NakadiTestUtils.switchFeature(Feature.DELETE_EVENT_TYPE_WITH_SUBSCRIPTIONS, false);

        when().delete("/event-types/{event-type}", etName)
                .then()
                .statusCode(HttpStatus.SC_CONFLICT);
    }

    @Test
    public void testDeleteEventTypeRestrictionFeatureToggle() throws Exception {
        final String etName = createEventType().getName();
        createSubscriptionForEventType(etName);

        NakadiTestUtils.switchFeature(Feature.DELETE_EVENT_TYPE_WITH_SUBSCRIPTIONS, true);

        when().delete("/event-types/{event-type}", etName)
                .then()
                .statusCode(HttpStatus.SC_OK);
    }

    @Test
    public void testDeleteEventTypeRestrictionDeletable() throws Exception {
        final String etName = createEventType().getName();

        final SubscriptionBase subscriptionBase = RandomSubscriptionBuilder.builder()
                .withEventType(etName)
                .withOwningApplication(DELETABLE_OWNING_APP)
                .withConsumerGroup(DELETABLE_CONSUMER_GROUP)
                .buildSubscriptionBase();
        NakadiTestUtils.createSubscription(subscriptionBase);

        NakadiTestUtils.switchFeature(Feature.DELETE_EVENT_TYPE_WITH_SUBSCRIPTIONS, false);

        when().delete("/event-types/{event-type}", etName)
                .then()
                .statusCode(HttpStatus.SC_OK);
    }

    @Test
    public void whenStatsOnNotInitializedSubscriptionThenCorrectResponse() throws IOException {
        final String et = createEventType().getName();
        final Subscription s = createSubscriptionForEventType(et);
        final Response response = when().get("/subscriptions/{sid}/stats", s.getId())
                .thenReturn();
        final ItemsWrapper<SubscriptionEventTypeStats> statsItems = MAPPER.readValue(
                response.print(),
                new TypeReference<ItemsWrapper<SubscriptionEventTypeStats>>() {
                });
        Assert.assertEquals(1, statsItems.getItems().size());
        final SubscriptionEventTypeStats stats = statsItems.getItems().get(0);
        Assert.assertEquals(et, stats.getEventType());
        Assert.assertEquals(1, stats.getPartitions().size());
        for (final SubscriptionEventTypeStats.Partition partition : stats.getPartitions()) {
            Assert.assertNotNull(partition);
            Assert.assertNotNull(partition.getPartition());
            Assert.assertEquals("", partition.getStreamId());
            Assert.assertNull(partition.getUnconsumedEvents());
            Assert.assertEquals("unassigned", partition.getState());
        }
    }

    @Test
    public void whenLightStatsOnNotInitializedSubscriptionThenCorrectResponse() throws IOException {
        final String et = createEventType().getName();
        final Subscription s = createSubscriptionForEventType(et);
        final String owningApplication = s.getOwningApplication();
        final Response response = when()
                .get("/subscriptions?show_status=true&owning_application=" + owningApplication)
                .thenReturn();
        final ItemsWrapper<Subscription> subsItems = MAPPER.readValue(response.print(),
                new TypeReference<ItemsWrapper<Subscription>>() {
                });
        for (final Subscription subscription : subsItems.getItems()) {
            if (subscription.getId().equals(s.getId())) {
                Assert.assertNotNull(subscription.getStatus());
                Assert.assertEquals("unassigned", subscription.getStatus().get(0).getPartitions().get(0).getState());
                Assert.assertEquals("", subscription.getStatus().get(0).getPartitions().get(0).getStreamId());
                return;
            }
        }
        Assert.fail();
    }

    @Test
    public void whenLightStatsOnActiveSubscriptionThenCorrectResponse() throws IOException {
        final String et = createEventType().getName();
        final Subscription s = createSubscriptionForEventTypeFromBegin(et);
        final String owningApplication = s.getOwningApplication();

        publishEvents(et, 15, i -> "{\"foo\":\"bar\"}");

        final TestStreamingClient client = TestStreamingClient
                .create(URL, s.getId(), "max_uncommitted_events=20")
                .start();
        waitFor(() -> assertThat(client.getJsonBatches(), hasSize(15)));

        final Response response = when()
                .get("/subscriptions?show_status=true&owning_application=" + owningApplication)
                .thenReturn();
        final ItemsWrapper<Subscription> subsItems = MAPPER.readValue(response.print(),
                new TypeReference<ItemsWrapper<Subscription>>() {
                });
        for (final Subscription subscription : subsItems.getItems()) {
            if (subscription.getId().equals(s.getId())) {
                Assert.assertNotNull(subscription.getStatus());
                Assert.assertEquals("assigned", subscription.getStatus().get(0).getPartitions().get(0).getState());
                Assert.assertEquals(client.getSessionId(),
                        subscription.getStatus().get(0).getPartitions().get(0).getStreamId());
                Assert.assertEquals(SubscriptionEventTypeStats.Partition.AssignmentType.AUTO,
                        subscription.getStatus().get(0).getPartitions().get(0).getAssignmentType());
                return;
            }
        }
        Assert.fail();
    }

    @Test
    public void whenStreamDuplicatePartitionsThenUnprocessableEntity() throws IOException {
        final String et = createEventType().getName();
        final Subscription s = createSubscriptionForEventType(et);

        final String body = "{\"partitions\":[" +
                "{\"event_type\":\"et1\",\"partition\":\"0\"}," +
                "{\"event_type\":\"et1\",\"partition\":\"0\"}]}";
        given().body(body)
                .contentType(JSON)
                .when()
                .post("/subscriptions/{sid}/events", s.getId())
                .then()
                .statusCode(HttpStatus.SC_UNPROCESSABLE_ENTITY)
                .body(JSON_HELPER.matchesObject(Problem.valueOf(
                        UNPROCESSABLE_ENTITY,
                        "Duplicated partition specified")));
    }

    @Test
    public void whenStreamWrongPartitionsThenUnprocessableEntity() throws IOException {
        final String et = createEventType().getName();
        final Subscription s = createSubscriptionForEventType(et);

        final String body = "{\"partitions\":[" +
                "{\"event_type\":\"" + et + "\",\"partition\":\"0\"}," +
                "{\"event_type\":\"" + et + "\",\"partition\":\"1\"}," +
                "{\"event_type\":\"dummy-et-123\",\"partition\":\"0\"}]}";
        given().body(body)
                .contentType(JSON)
                .when()
                .post("/subscriptions/{sid}/events", s.getId())
                .then()
                .statusCode(HttpStatus.SC_UNPROCESSABLE_ENTITY)
                .body(JSON_HELPER.matchesObject(Problem.valueOf(
                        UNPROCESSABLE_ENTITY,
                        "Wrong partitions specified - some partitions don't belong to subscription: " +
                                "EventTypePartition{eventType='" + et + "', partition='1'}, " +
                                "EventTypePartition{eventType='dummy-et-123', partition='0'}")));
    }

    private Response commitCursors(final Subscription subscription, final String cursor, final String streamId) {
        return given()
                .body(cursor)
                .contentType(JSON)
                .header("X-Nakadi-StreamId", streamId)
                .post(format(CURSORS_URL, subscription.getId()));
    }

    private String getCommittedOffsetFromZk(
            final String eventType, final Subscription subscription, final String partition) throws Exception {
        final String path = format("/nakadi/subscriptions/{0}/offsets/{1}/{2}", subscription.getId(),
                eventType, partition);
        final byte[] data = CURATOR.getData().forPath(path);
        return new String(data, Charsets.UTF_8);
    }

}
