package org.zalando.nakadi.webservice;

import org.apache.http.HttpStatus;
import org.hamcrest.MatcherAssert;
import org.hamcrest.Matchers;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.zalando.nakadi.domain.Feature;
import org.zalando.nakadi.domain.Subscription;
import org.zalando.nakadi.utils.TestUtils;
import org.zalando.nakadi.webservice.utils.NakadiTestUtils;
import org.zalando.nakadi.webservice.utils.TestStreamingClient;

import java.util.List;
import java.util.Map;

import static com.jayway.restassured.RestAssured.given;

public class BinaryEventPublisherAT extends BaseAT {

    private static final String NAKADI_ACCESS_LOG = "nakadi.access.log";
    private static final String NAKADI_SUBSCRIPTION_LOG = "nakadi.subscription.log";
    private static final String NAKADI_EVENT_TYPE_LOG = "nakadi.event.type.log";
    private static final String NAKADI_BATCH_PUBLISHED = "nakadi.batch.published";

    @Before
    public void setupAvroForKPIEvents() {
        NakadiTestUtils.switchFeature(Feature.AVRO_FOR_KPI_EVENTS, true);
    }

    @Test
    public void testNakadiAccessLogInAvro() throws Exception {
        // lets read nakadi.access.log to validate if there is an event
        final Subscription subscription = NakadiTestUtils
                .createSubscriptionForEventType(NAKADI_ACCESS_LOG);

        final TestStreamingClient client = TestStreamingClient
                .create(subscription.getId())
                .start();

        // let log any request to nakadi.access.log event type
        final String path = "/event-types/" + NAKADI_ACCESS_LOG;
        given()
                .get(path)
                .then()
                .statusCode(HttpStatus.SC_OK);

        TestUtils.waitFor(() -> MatcherAssert.assertThat(
                client.getBatches().size(), Matchers.greaterThanOrEqualTo(1)), 10000);
        final List<Map> events = client.getBatches().get(0).getEvents();
        Assert.assertFalse(events.isEmpty());
        // when tests are run in parallel it is hard to get specific event,
        // that's why check that events are in the event type
        Assert.assertEquals(
                NAKADI_ACCESS_LOG,
                ((Map) events.get(0).get("metadata")).get("event_type"));
    }

    @Test
    public void testNakadiSubscriptionLogInAvro() throws Exception {
        final Subscription subscription = NakadiTestUtils
                .createSubscriptionForEventType(NAKADI_SUBSCRIPTION_LOG);

        final TestStreamingClient client = TestStreamingClient
                .create(subscription.getId())
                .start();

        NakadiTestUtils.createSubscriptionForEventType(NAKADI_ACCESS_LOG);

        final var event = consumeEvent(client);
        // when tests are run in parallel it is hard to get specific event,
        // that's why check that events are in the event type
        Assert.assertEquals(
                NAKADI_SUBSCRIPTION_LOG,
                ((Map) event.get("metadata")).get("event_type"));
        Assert.assertEquals("created", event.get("status"));
    }

    @Test
    public void testNakadiEventTypeLogInAvro() throws Exception {
        final Subscription subscription = NakadiTestUtils
                .createSubscriptionForEventType(NAKADI_EVENT_TYPE_LOG);

        final TestStreamingClient client = TestStreamingClient
                .create(subscription.getId())
                .start();

        NakadiTestUtils.createEventType();

        final var event = consumeEvent(client);

        Assert.assertEquals(
                NAKADI_EVENT_TYPE_LOG,
                ((Map) event.get("metadata")).get("event_type"));
        Assert.assertEquals("created", event.get("status"));
    }

    @Test
    public void testNakadiBatchPublishedInAvro() throws Exception {
        final Subscription subscription = NakadiTestUtils
                .createSubscriptionForEventType(NAKADI_BATCH_PUBLISHED);

        final TestStreamingClient client = TestStreamingClient
                .create(subscription.getId())
                .start();

        final var et = NakadiTestUtils.createEventType();
        NakadiTestUtils.publishEvent(et.getName(), "{\"foo\":\"bar\"}");

        final var event = consumeEvent(client);
        Assert.assertEquals(
                NAKADI_BATCH_PUBLISHED,
                ((Map) event.get("metadata")).get("event_type"));
    }

    private Map consumeEvent(final TestStreamingClient client) {
        TestUtils.waitFor(() -> MatcherAssert.assertThat(
                client.getBatches().size(), Matchers.greaterThanOrEqualTo(1)), 10000);
        final List<Map> events = client.getBatches().get(0).getEvents();
        Assert.assertFalse(events.isEmpty());
        return events.get(0);
    }
}
