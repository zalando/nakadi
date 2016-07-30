package org.zalando.nakadi.webservice;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.jayway.restassured.response.Response;
import org.zalando.nakadi.config.JsonConfig;
import org.zalando.nakadi.domain.Cursor;
import org.zalando.nakadi.domain.EventType;
import org.zalando.nakadi.domain.Subscription;
import org.zalando.nakadi.domain.SubscriptionListWrapper;
import org.zalando.nakadi.utils.JsonTestHelper;
import org.zalando.nakadi.webservice.utils.ZookeeperTestUtils;
import org.apache.curator.framework.CuratorFramework;
import org.apache.http.HttpStatus;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Set;

import static com.jayway.restassured.RestAssured.get;
import static com.jayway.restassured.RestAssured.given;
import static com.jayway.restassured.http.ContentType.JSON;
import static java.text.MessageFormat.format;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.isEmptyString;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.zalando.nakadi.utils.TestUtils.buildDefaultEventType;
import static org.zalando.nakadi.webservice.utils.NakadiTestUtils.createSubscription;

public class SubscriptionAT extends BaseAT {

    private static final String SUBSCRIPTIONS_URL = "/subscriptions";
    private static final String SUBSCRIPTION_URL = "/subscriptions/{0}";
    private static final String CURSORS_URL = "/subscriptions/{0}/cursors";

    private static final ObjectMapper MAPPER = (new JsonConfig()).jacksonObjectMapper();
    private static final JsonTestHelper JSON_HELPER = new JsonTestHelper(MAPPER);

    @Test
    public void testSubscriptionBaseOperations() throws IOException {
        // create event type in Nakadi
        final EventType eventType = createEventType();

        // create subscription
        final String subscription = "{\"owning_application\":\"app\",\"event_types\":[\"" + eventType.getName() + "\"]}";
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
        final Set<String> eventTypes = ImmutableSet.of(createEventType().getName());
        final Subscription sub1 = createSubscription(eventTypes, "app");
        final Subscription sub2 = createSubscription(eventTypes, "app");
        createSubscription(eventTypes, "anotherApp");

        final SubscriptionListWrapper expectedList = new SubscriptionListWrapper(ImmutableList.of(sub1, sub2));

        given()
                .param("owning_application", "app")
                .get("/subscriptions")
                .then()
                .statusCode(HttpStatus.SC_OK)
                .body(JSON_HELPER.matchesObject(expectedList));
    }

    @Test
    public void testOffsetsCommit() throws Exception {
        // create event type in Nakadi
        final EventType eventType = createEventType();
        final String topic = EVENT_TYPE_REPO.findByName(eventType.getName()).getTopic();

        final Subscription subscription = createSubscription(ImmutableSet.of(eventType.getName()));

        commitCursors(subscription, "[{\"partition\":\"0\",\"offset\":\"25\"}]")
                .then()
                .statusCode(HttpStatus.SC_OK);

        // check that offset is actually committed to Zookeeper
        final CuratorFramework curator = ZookeeperTestUtils.createCurator(ZOOKEEPER_URL);
        String committedOffset = getCommittedOffsetFromZk(topic, subscription, "0", curator);
        assertThat(committedOffset, equalTo("25"));

        // commit lower offsets and expect 204
        commitCursors(subscription, "[{\"partition\":\"0\",\"offset\":\"10\"}]")
                .then()
                .statusCode(HttpStatus.SC_NO_CONTENT);

        // check that committed offset in Zookeeper is not changed
        committedOffset = getCommittedOffsetFromZk(topic, subscription, "0", curator);
        assertThat(committedOffset, equalTo("25"));
    }

    @Test
    public void testGetSubscriptionCursors() throws IOException {
        final EventType eventType = createEventType();
        final Subscription subscription = createSubscription(ImmutableSet.of(eventType.getName()));
        commitCursors(subscription, "[{\"partition\":\"0\",\"offset\":\"25\"}]")
                .then()
                .statusCode(HttpStatus.SC_OK);

        final List<Cursor> actualCursors = getSubscriptionCursors(subscription);
        Assert.assertEquals(Arrays.asList(new Cursor("0", "25")), actualCursors);
    }

    @Test
    public void testGetSubscriptionCursorsEmpty() throws IOException {
        final EventType eventType = createEventType();
        final Subscription subscription = createSubscription(ImmutableSet.of(eventType.getName()));

        Assert.assertTrue(getSubscriptionCursors(subscription).isEmpty());
    }

    @Test
    public void testGetSubscriptionNotFound() throws IOException {
        given()
                .get(format(CURSORS_URL, "UNKNOWN_SUB_ID"))
                .then()
                .statusCode(HttpStatus.SC_NOT_FOUND);
    }

    private Response commitCursors(final Subscription subscription, final String cursor) {
        return given()
                .body(cursor)
                .contentType(JSON)
                .put(format(CURSORS_URL, subscription.getId()));
    }

    private List<Cursor> getSubscriptionCursors(final Subscription subscription) throws IOException {
        final Response response = given().get(format(CURSORS_URL, subscription.getId()));
        return MAPPER.readValue(response.print(), new TypeReference<List<Cursor>>() {});
    }

    private String getCommittedOffsetFromZk(final String topic, final Subscription subscription,
                                            final String partition, final CuratorFramework curator) throws Exception {
        final String path = format("/nakadi/subscriptions/{0}/topics/{1}/{2}/offset", subscription.getId(),
                topic, partition);
        final byte[] data = curator.getData().forPath(path);
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
