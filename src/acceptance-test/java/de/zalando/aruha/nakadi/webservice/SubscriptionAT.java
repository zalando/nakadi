package de.zalando.aruha.nakadi.webservice;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableSet;
import com.jayway.restassured.response.Response;
import de.zalando.aruha.nakadi.config.JsonConfig;
import de.zalando.aruha.nakadi.domain.EventType;
import de.zalando.aruha.nakadi.domain.Subscription;
import de.zalando.aruha.nakadi.webservice.utils.ZookeeperTestUtils;
import org.apache.curator.framework.CuratorFramework;
import org.apache.http.HttpStatus;
import org.junit.Test;

import java.io.IOException;

import static com.jayway.restassured.RestAssured.given;
import static com.jayway.restassured.http.ContentType.JSON;
import static de.zalando.aruha.nakadi.utils.TestUtils.buildDefaultEventType;
import static java.text.MessageFormat.format;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.isEmptyString;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.core.IsEqual.equalTo;

public class SubscriptionAT extends BaseAT {

    private static final String SUBSCRIPTIONS_URL = "/subscriptions";
    private static final String CURSORS_URL = "/subscriptions/{0}/cursors";

    private static final ObjectMapper mapper = (new JsonConfig()).jacksonObjectMapper();

    @Test
    public void testSubscriptionCreation() throws IOException {
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
        final Subscription subFirst = mapper.readValue(response.print(), Subscription.class);

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
        final Subscription subSecond = mapper.readValue(response.print(), Subscription.class);
        assertThat(subSecond, equalTo(subFirst));
    }

    @Test
    public void testOffsetsCommit() throws Exception {
        // create event type in Nakadi
        final EventType eventType = createEventType();

        // create subscription
        final String subscriptionJson = "{\"owning_application\":\"app\",\"event_types\":[\"" + eventType.getName() + "\"]}";
        Response response = given()
                .body(subscriptionJson)
                .contentType(JSON)
                .post(SUBSCRIPTIONS_URL);
        final Subscription subscription = mapper.readValue(response.print(), Subscription.class);

        // commit offsets and expect 200
        given()
                .body("[{\"partition\":\"0\",\"offset\":\"25\"}]")
                .contentType(JSON)
                .put(format(CURSORS_URL, subscription.getId()))
                .then()
                .statusCode(HttpStatus.SC_OK);

        // check that offset is actually committed to Zookeeper
        final CuratorFramework curator = ZookeeperTestUtils.createCurator(zookeeperUrl);
        String committedOffset = getCommittedOffsetFromZk(eventType, subscription, "0", curator);
        assertThat(committedOffset, equalTo("25"));

        // commit lower offsets and expect 204
        given()
                .body("[{\"partition\":\"0\",\"offset\":\"10\"}]")
                .contentType(JSON)
                .put(format(CURSORS_URL, subscription.getId()))
                .then()
                .statusCode(HttpStatus.SC_NO_CONTENT);

        // check that committed offset in Zookeeper is not changed
        committedOffset = getCommittedOffsetFromZk(eventType, subscription, "0", curator);
        assertThat(committedOffset, equalTo("25"));
    }

    private String getCommittedOffsetFromZk(final EventType eventType, final Subscription subscription,
                                            final String partition, final CuratorFramework curator) throws Exception {
        final String path = format("/nakadi/subscriptions/{0}/topics/{1}/{2}/offset", subscription.getId(),
                eventType.getName(), partition);
        final byte[] data = curator.getData().forPath(path);
        return new String(data, Charsets.UTF_8);
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
