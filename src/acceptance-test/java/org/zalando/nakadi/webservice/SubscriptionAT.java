package org.zalando.nakadi.webservice;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.jayway.restassured.response.Response;
import org.apache.curator.framework.CuratorFramework;
import org.apache.http.HttpStatus;
import org.apache.zookeeper.data.Stat;
import org.junit.Assert;
import org.junit.Test;
import org.zalando.nakadi.config.JsonConfig;
import org.zalando.nakadi.domain.EventType;
import org.zalando.nakadi.domain.ItemsWrapper;
import org.zalando.nakadi.domain.PaginationLinks;
import org.zalando.nakadi.domain.Subscription;
import org.zalando.nakadi.view.SubscriptionCursor;
import org.zalando.nakadi.domain.PaginationWrapper;
import org.zalando.nakadi.utils.JsonTestHelper;
import org.zalando.nakadi.utils.RandomSubscriptionBuilder;
import org.zalando.nakadi.webservice.utils.TestStreamingClient;
import org.zalando.nakadi.webservice.utils.ZookeeperTestUtils;
import org.zalando.problem.Problem;
import org.zalando.problem.ThrowableProblem;

import java.io.IOException;
import java.util.List;

import static com.jayway.restassured.RestAssured.get;
import static com.jayway.restassured.RestAssured.given;
import static com.jayway.restassured.RestAssured.when;
import static com.jayway.restassured.http.ContentType.JSON;
import static java.text.MessageFormat.format;
import static javax.ws.rs.core.Response.Status.CONFLICT;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.isEmptyString;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.zalando.nakadi.utils.TestUtils.buildDefaultEventType;
import static org.zalando.nakadi.utils.TestUtils.randomUUID;
import static org.zalando.nakadi.utils.TestUtils.waitFor;
import static org.zalando.nakadi.webservice.utils.NakadiTestUtils.createSubscription;
import static org.zalando.nakadi.webservice.utils.NakadiTestUtils.createSubscriptionForEventType;
import static org.zalando.nakadi.webservice.utils.TestStreamingClient.SESSION_ID_UNKNOWN;

public class SubscriptionAT extends BaseAT {

    private static final String SUBSCRIPTIONS_URL = "/subscriptions";
    private static final String SUBSCRIPTION_URL = "/subscriptions/{0}";
    private static final String CURSORS_URL = "/subscriptions/{0}/cursors";

    private static final ObjectMapper MAPPER = (new JsonConfig()).jacksonObjectMapper();
    private static final JsonTestHelper JSON_HELPER = new JsonTestHelper(MAPPER);
    private static final CuratorFramework CURATOR = ZookeeperTestUtils.createCurator(ZOOKEEPER_URL);

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
    public void testListSubscriptions() throws IOException {
        final String etName = createEventType().getName();

        final String filterApp = randomUUID();
        final Subscription sub1 = createSubscription(RandomSubscriptionBuilder.builder()
                .withEventType(etName).withOwningApplication(filterApp).buildSubscriptionBase());
        final Subscription sub2 = createSubscription(RandomSubscriptionBuilder.builder()
                .withEventType(etName).withOwningApplication(filterApp).buildSubscriptionBase());
        createSubscription(RandomSubscriptionBuilder.builder().withEventType(etName).buildSubscriptionBase());

        final PaginationWrapper expectedList = new PaginationWrapper(ImmutableList.of(sub2, sub1),
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
        final String topic = EVENT_TYPE_REPO.findByName(etName).getTopic();

        final Subscription subscription = createSubscriptionForEventType(etName);

        final TestStreamingClient client = TestStreamingClient
                .create(URL, subscription.getId(), "")
                .start();
        waitFor(() -> assertThat(client.getSessionId(), not(equalTo(SESSION_ID_UNKNOWN))));

        String cursor = "{\"items\":[{\"partition\":\"0\",\"offset\":\"25\",\"event_type\":\"" + etName +
                "\",\"cursor_token\":\"abc\"}]}";
        commitCursors(subscription, cursor, client.getSessionId())
                .then()
                .statusCode(HttpStatus.SC_NO_CONTENT);

        // check that offset is actually committed to Zookeeper
        String committedOffset = getCommittedOffsetFromZk(topic, subscription, "0");
        assertThat(committedOffset, equalTo("25"));

        // commit lower offsets and expect 200
        cursor = "{\"items\":[{\"partition\":\"0\",\"offset\":\"10\",\"event_type\":\"" + etName +
                "\",\"cursor_token\":\"abc\"}]}";
        commitCursors(subscription, cursor, client.getSessionId())
                .then()
                .statusCode(HttpStatus.SC_OK);

        // check that committed offset in Zookeeper is not changed
        committedOffset = getCommittedOffsetFromZk(topic, subscription, "0");
        assertThat(committedOffset, equalTo("25"));
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

        final List<SubscriptionCursor> actualCursors = getSubscriptionCursors(subscription).getItems();
        assertThat(actualCursors, hasSize(1));

        final SubscriptionCursor actualCursor = actualCursors.get(0);
        assertThat(actualCursor.getPartition(), equalTo("0"));
        assertThat(actualCursor.getOffset(), equalTo("25"));
        assertThat(actualCursor.getEventType(), equalTo(etName));
    }

    @Test
    public void testGetSubscriptionCursorsEmpty() throws IOException {
        final String etName = createEventType().getName();
        final Subscription subscription = createSubscriptionForEventType(etName);
        Assert.assertTrue(getSubscriptionCursors(subscription).getItems().isEmpty());
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

        when().delete("/subscriptions/{sid}", subscription.getId())
                .then()
                .statusCode(HttpStatus.SC_NO_CONTENT);

        when().get("/subscriptions/{sid}", subscription.getId())
                .then()
                .statusCode(HttpStatus.SC_NOT_FOUND);

        final Stat stat = CURATOR.checkExists().forPath(format("/nakadi/subscriptions/{0}", subscription.getId()));
        final boolean subscriptionExistsInZk = stat != null;
        assertThat(subscriptionExistsInZk, is(false));
    }

    @Test
    public void testDeleteEventTypeRestriction() throws Exception {
        final String etName = createEventType().getName();
        createSubscriptionForEventType(etName);

        final ThrowableProblem expectedProblem = Problem.valueOf(CONFLICT,
                "Not possible to remove event-type as it has subscriptions");

        when().delete("/event-types/{event-type}", etName)
                .then()
                .statusCode(HttpStatus.SC_CONFLICT)
                .body(JSON_HELPER.matchesObject(expectedProblem));
    }

    private Response commitCursors(final Subscription subscription, final String cursor, final String streamId) {
        return given()
                .body(cursor)
                .contentType(JSON)
                .header("X-Nakadi-StreamId", streamId)
                .post(format(CURSORS_URL, subscription.getId()));
    }

    private ItemsWrapper<SubscriptionCursor> getSubscriptionCursors(final Subscription subscription)
            throws IOException {
        final Response response = given().get(format(CURSORS_URL, subscription.getId()));
        return MAPPER.readValue(response.print(), new TypeReference<ItemsWrapper<SubscriptionCursor>>() {});
    }

    private String getCommittedOffsetFromZk(final String topic, final Subscription subscription, final String partition)
            throws Exception {
        final String path = format("/nakadi/subscriptions/{0}/topics/{1}/{2}/offset", subscription.getId(),
                topic, partition);
        final byte[] data = CURATOR.getData().forPath(path);
        return new String(data, Charsets.UTF_8);
    }

    private EventType createEventType() throws JsonProcessingException {
        final EventType eventType = buildDefaultEventType();
        given()
                .body(MAPPER.writeValueAsString(eventType))
                .contentType(JSON)
                .post("/event-types");
        return eventType;
    }

}
