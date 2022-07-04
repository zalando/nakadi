package org.zalando.nakadi.webservice;

import org.apache.http.HttpStatus;
import org.hamcrest.MatcherAssert;
import org.hamcrest.Matchers;
import org.junit.Assert;
import org.junit.Test;
import org.zalando.nakadi.domain.Subscription;
import org.zalando.nakadi.utils.TestUtils;
import org.zalando.nakadi.webservice.utils.NakadiTestUtils;
import org.zalando.nakadi.webservice.utils.TestStreamingClient;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static com.jayway.restassured.RestAssured.given;

public class BinaryEventPublisherAT extends BaseAT {

    private static final String NAKADI_ACCESS_LOG = "nakadi.access.log";
    private static final String NAKADI_SUBSCRIPTION_LOG = "nakadi.subscription.log";
    private static final String NAKADI_EVENT_TYPE_LOG = "nakadi.event.type.log";
    private static final String NAKADI_BATCH_PUBLISHED = "nakadi.batch.published";
    private static final String NAKADI_DATA_STREAMED = "nakadi.data.streamed";

    @Test
    public void testNakadiAccessLogInAvro() throws Exception {
        // lets read nakadi.access.log to validate if there is an event
        final var client = subscribeAndStartStreaming(NAKADI_ACCESS_LOG);

        // let log any request to nakadi.access.log event type
        final String path = "/event-types/" + NAKADI_ACCESS_LOG;
        given()
                .get(path)
                .then()
                .statusCode(HttpStatus.SC_OK);

        final var events = consumeEvent(client);
        Assert.assertFalse(events.isEmpty());
        final var event = events.get(0);
        // All acceptance tests are run against same instance, so the exact event that is consumed is unpredictable.
        // So the test is only looking for a valid event.
        Assert.assertEquals(
                NAKADI_ACCESS_LOG,
                ((Map) event.get("metadata")).get("event_type"));
        Assert.assertNotNull(event.get("method"));
        Assert.assertNotNull(event.get("path"));
        Assert.assertNotNull(event.get("query"));
        Assert.assertNotNull(event.get("user_agent"));
        Assert.assertNotNull(event.get("app"));
        Assert.assertNotNull(event.get("app_hashed"));
        Assert.assertNotNull(event.get("status_code"));
        Assert.assertNotNull(event.get("response_time_ms"));
        Assert.assertNotNull(event.get("accept_encoding"));
        Assert.assertNotNull(event.get("content_encoding"));
        Assert.assertNotNull(event.get("request_length"));
        Assert.assertNotNull(event.get("response_length"));
        Assert.assertNull(event.get("random_key"));
    }

    @Test
    public void testNakadiSubscriptionLogInAvro() throws Exception {
        // Subscribing to subscription log
        final var client = subscribeAndStartStreaming(NAKADI_SUBSCRIPTION_LOG);

        // Creating a subscription to trigger a subscription log event
        NakadiTestUtils.createSubscriptionForEventType(NAKADI_ACCESS_LOG);

        // Consume and check the subscription log event
        final var events = consumeEvent(client);
        Assert.assertFalse(events.isEmpty());
        final var event = events.get(0);
        // All acceptance tests are run against same instance, so the exact event that is consumed is unpredictable.
        // So the test is only looking for a valid event.
        Assert.assertEquals(
                NAKADI_SUBSCRIPTION_LOG,
                ((Map) event.get("metadata")).get("event_type"));
        Assert.assertEquals("created", event.get("status"));
        Assert.assertNotNull(event.get("subscription_id"));
    }

    @Test
    public void testNakadiEventTypeLogInAvro() throws Exception {
        // Subscribe to event type log
        final var client = subscribeAndStartStreaming(NAKADI_EVENT_TYPE_LOG);

        // Create an event type to force an event to event type log
        NakadiTestUtils.createEventType();

        final var events = consumeEvent(client);
        Assert.assertFalse(events.isEmpty());
        final var event = events.get(0);

        // All acceptance tests are run against same instance, so the exact event that is consumed is unpredictable.
        // So the test is only looking for a valid event.
        Assert.assertEquals(
                NAKADI_EVENT_TYPE_LOG,
                ((Map) event.get("metadata")).get("event_type"));
        Assert.assertNotNull(event.get("event_type"));
        Assert.assertNotNull(event.get("status"));
        Assert.assertNotNull(event.get("category"));
        Assert.assertNotNull(event.get("authz"));
        Assert.assertNotNull(event.get("compatibility_mode"));
    }

    @Test
    public void testNakadiBatchPublishedInAvro() throws Exception {
        // Subscribe to batch publish log
        final var client = subscribeAndStartStreaming(NAKADI_BATCH_PUBLISHED);

        // Create an event type and publish something to force an event to batch publish log
        final var et = NakadiTestUtils.createEventType();
        NakadiTestUtils.publishEvent(et.getName(), "{\"foo\":\"bar\"}");

        final var events = consumeEvent(client);
        Assert.assertFalse(events.isEmpty());
        final var event = events.get(0);
        // All acceptance tests are run against same instance, so the exact event that is consumed is unpredictable.
        // So the test is only looking for a valid event.
        Assert.assertEquals(
                NAKADI_BATCH_PUBLISHED,
                ((Map) event.get("metadata")).get("event_type"));
        Assert.assertNotNull(event.get("event_type"));
        Assert.assertNotNull(event.get("app"));
        Assert.assertNotNull(event.get("app_hashed"));
        Assert.assertNotNull(event.get("token_realm"));
        Assert.assertNotNull(event.get("number_of_events"));
        Assert.assertNotNull(event.get("ms_spent"));
        Assert.assertNotNull(event.get("batch_size"));
    }

    @Test
    public void testNakadiDataStreamedInAvro() throws Exception {
        final var client = subscribeAndStartStreaming(NAKADI_DATA_STREAMED);

        // Subscribe to nakadi.event.type.log, and force consumption by creating an event type
        // This will force an event to nakadi.data.streamed
        final var dummyClient = subscribeAndStartStreaming(NAKADI_EVENT_TYPE_LOG);
        NakadiTestUtils.createEventType();
        consumeEvent(dummyClient);

        final var events = consumeEvent(client);
        Assert.assertFalse(events.isEmpty());
        final var event = events.get(0);
        // All acceptance tests are run against same instance, so the exact event that is consumed is unpredictable.
        // So the test is only looking for a valid event.
        final var metadata = (Map) event.get("metadata");
        Assert.assertEquals(NAKADI_DATA_STREAMED, metadata.get("event_type"));
        Assert.assertNotNull(metadata.get("occurred_at"));
        Assert.assertNotNull(metadata.get("received_at"));
        Assert.assertNotNull(metadata.get("partition"));
        Assert.assertNotNull(metadata.get("flow_id"));
        Assert.assertNotNull(metadata.get("partition"));
        Assert.assertNotNull(event.get("api"));
        Assert.assertNotNull(event.get("event_type"));
        Assert.assertNotNull(event.get("app"));
        Assert.assertNotNull(event.get("app_hashed"));
        Assert.assertNotNull(event.get("token_realm"));
        Assert.assertNotNull(event.get("number_of_events"));
        Assert.assertNotNull(event.get("bytes_streamed"));
        Assert.assertNotNull(event.get("batches_streamed"));
    }

    private List<Map> consumeEvent(final TestStreamingClient client) {
        TestUtils.waitFor(() -> MatcherAssert.assertThat(
                client.getJsonBatches().size(), Matchers.greaterThanOrEqualTo(1)), 10000);
        return client.getJsonBatches().get(0).getEvents();
    }

    private TestStreamingClient subscribeAndStartStreaming(final String eventType) throws IOException {
        final Subscription subscription = NakadiTestUtils.createSubscriptionForEventType(eventType);
        return TestStreamingClient.create(subscription.getId()).start();
    }
}
