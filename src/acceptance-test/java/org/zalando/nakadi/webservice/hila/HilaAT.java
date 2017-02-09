package org.zalando.nakadi.webservice.hila;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.http.HttpStatus;
import org.hamcrest.core.StringContains;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.zalando.nakadi.config.JsonConfig;
import org.zalando.nakadi.view.Cursor;
import org.zalando.nakadi.domain.EventType;
import org.zalando.nakadi.domain.ItemsWrapper;
import org.zalando.nakadi.domain.Subscription;
import org.zalando.nakadi.domain.SubscriptionBase;
import org.zalando.nakadi.view.SubscriptionCursor;
import org.zalando.nakadi.domain.SubscriptionEventTypeStats;
import org.zalando.nakadi.service.BlacklistService;
import org.zalando.nakadi.utils.JsonTestHelper;
import org.zalando.nakadi.utils.RandomSubscriptionBuilder;
import org.zalando.nakadi.webservice.BaseAT;
import org.zalando.nakadi.webservice.SettingsControllerAT;
import org.zalando.nakadi.webservice.utils.NakadiTestUtils;
import org.zalando.nakadi.webservice.utils.TestStreamingClient;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.stream.IntStream;

import static com.jayway.restassured.RestAssured.given;
import static com.jayway.restassured.RestAssured.when;
import static java.text.MessageFormat.format;
import static java.util.stream.IntStream.range;
import static java.util.stream.IntStream.rangeClosed;
import static org.apache.http.HttpStatus.SC_CONFLICT;
import static org.apache.http.HttpStatus.SC_NO_CONTENT;
import static org.apache.http.HttpStatus.SC_OK;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.not;
import static org.zalando.nakadi.domain.SubscriptionBase.InitialPosition.BEGIN;
import static org.zalando.nakadi.utils.TestUtils.waitFor;
import static org.zalando.nakadi.webservice.hila.StreamBatch.MatcherIgnoringToken.equalToBatchIgnoringToken;
import static org.zalando.nakadi.webservice.hila.StreamBatch.singleEventBatch;
import static org.zalando.nakadi.webservice.utils.NakadiTestUtils.commitCursors;
import static org.zalando.nakadi.webservice.utils.NakadiTestUtils.createEventType;
import static org.zalando.nakadi.webservice.utils.NakadiTestUtils.createSubscription;
import static org.zalando.nakadi.webservice.utils.NakadiTestUtils.publishEvent;
import static org.zalando.nakadi.webservice.utils.TestStreamingClient.SESSION_ID_UNKNOWN;

public class HilaAT extends BaseAT {

    private static final JsonTestHelper JSON_TEST_HELPER = new JsonTestHelper(new JsonConfig().jacksonObjectMapper());
    private EventType eventType;
    private Subscription subscription;

    @Before
    public void before() throws IOException {
        // create event-type and subscribe to it
        eventType = createEventType();
        final SubscriptionBase subscription = RandomSubscriptionBuilder.builder()
                .withEventType(eventType.getName())
                .withStartFrom(BEGIN)
                .buildSubscriptionBase();
        this.subscription = createSubscription(subscription);
    }

    @Test(timeout = 30000)
    public void whenOffsetIsCommittedNextSessionStartsFromNextEventAfterCommitted() throws Exception {
        // write 4 events to event-type
        rangeClosed(0, 3)
                .forEach(x -> publishEvent(eventType.getName(), "{\"foo\":\"bar" + x + "\"}"));

        // create session, read from subscription and wait for events to be sent
        final TestStreamingClient client = TestStreamingClient
                .create(URL, subscription.getId(), "stream_limit=2")
                .start();
        waitFor(() -> assertThat(client.getBatches(), hasSize(2)));
        assertThat(client.getBatches().get(0), equalToBatchIgnoringToken(singleEventBatch("0", "0", eventType.getName(),
                ImmutableMap.of("foo", "bar0"), "Stream started")));
        assertThat(client.getBatches().get(1), equalToBatchIgnoringToken(singleEventBatch("0", "1", eventType.getName(),
                ImmutableMap.of("foo", "bar1"))));

        // commit offset that will also trigger session closing as we reached stream_limit and committed
        commitCursors(subscription.getId(), ImmutableList.of(client.getBatches().get(1).getCursor()),
                client.getSessionId());
        waitFor(() -> assertThat(client.isRunning(), is(false)));

        // create new session and read from subscription again
        client.start();
        waitFor(() -> assertThat(client.getBatches(), hasSize(2)));

        // check that we have read the next two events with correct offsets
        assertThat(client.getBatches().get(0), equalToBatchIgnoringToken(singleEventBatch("0", "2", eventType.getName(),
                ImmutableMap.of("foo", "bar2"), "Stream started")));
        assertThat(client.getBatches().get(1), equalToBatchIgnoringToken(singleEventBatch("0", "3", eventType.getName(),
                ImmutableMap.of("foo", "bar3"))));
    }


    @Test(timeout = 5000)
    public void whenNoEventsThenFirstOffsetIsBEGIN() throws Exception {
        final TestStreamingClient client = TestStreamingClient
                .create(URL, subscription.getId(), "batch_flush_timeout=1")
                .start();
        waitFor(() -> assertThat(client.getBatches(), not(empty())));
        assertThat(client.getBatches().get(0).getCursor().getOffset(), equalTo(Cursor.BEFORE_OLDEST_OFFSET));
    }

    @Test(timeout = 5000)
    public void whenNoEventsThenBeginOffsetIsUsed() throws Exception {
        final TestStreamingClient client = TestStreamingClient
                .create(URL, subscription.getId(), "")
                .start();
        waitFor(() -> assertThat(client.getSessionId(), not(equalTo(SESSION_ID_UNKNOWN))));

        when().get("/subscriptions/{sid}/cursors", subscription.getId())
                .then()
                .body("items[0].offset", equalTo(Cursor.BEFORE_OLDEST_OFFSET));

        final int commitResult = commitCursors(subscription.getId(),
                ImmutableList.of(new SubscriptionCursor("0", Cursor.BEFORE_OLDEST_OFFSET, eventType.getName(), "abc")),
                client.getSessionId());
        assertThat(commitResult, equalTo(SC_OK));
    }

    @Test(timeout = 5000)
    public void whenCommitVeryFirstEventThenOk() throws Exception {
        publishEvent(eventType.getName(), "{\"foo\":\"bar\"}");

        // create session, read from subscription and wait for events to be sent
        final TestStreamingClient client = TestStreamingClient
                .create(URL, subscription.getId(), "")
                .start();
        waitFor(() -> assertThat(client.getBatches(), not(empty())));

        // commit and check that status is 204
        final int commitResult = commitCursors(subscription.getId(),
                ImmutableList.of(new SubscriptionCursor("0", "0", eventType.getName(), "token")),
                client.getSessionId());
        assertThat(commitResult, equalTo(SC_NO_CONTENT));
    }

    @Test(timeout = 15000)
    public void whenWindowSizeIsSetItIsConsidered() throws Exception {

        range(0, 15).forEach(x -> publishEvent(eventType.getName(), "{\"foo\":\"bar\"}"));

        final TestStreamingClient client = TestStreamingClient
                .create(URL, subscription.getId(), "max_uncommitted_events=5")
                .start();

        waitFor(() -> assertThat(client.getBatches(), hasSize(5)));

        SubscriptionCursor cursorToCommit = client.getBatches().get(4).getCursor();
        commitCursors(subscription.getId(), ImmutableList.of(cursorToCommit), client.getSessionId());

        waitFor(() -> assertThat(client.getBatches(), hasSize(10)));

        cursorToCommit = client.getBatches().get(6).getCursor();
        commitCursors(subscription.getId(), ImmutableList.of(cursorToCommit), client.getSessionId());

        waitFor(() -> assertThat(client.getBatches(), hasSize(12)));
    }

    @Test(timeout = 15000)
    public void whenCommitTimeoutReachedSessionIsClosed() throws Exception {

        publishEvent(eventType.getName(), "{\"foo\":\"bar\"}");

        final TestStreamingClient client = TestStreamingClient
                .create(URL, subscription.getId(), "") // commit_timeout is 5 seconds for test
                .start();

        waitFor(() -> assertThat(client.getBatches(), hasSize(2)), 10000);
        waitFor(() -> assertThat(client.isRunning(), is(false)), 10000);
        assertThat(client.getBatches().get(1), equalToBatchIgnoringToken(singleEventBatch("0", "0", eventType.getName(),
                ImmutableMap.of(), "Commit timeout reached")));
    }

    @Test(timeout = 15000)
    public void whenStreamTimeoutReachedSessionIsClosed() throws Exception {

        publishEvent(eventType.getName(), "{\"foo\":\"bar\"}");

        final TestStreamingClient client = TestStreamingClient
                .create(URL, subscription.getId(), "stream_timeout=3")
                .start();

        waitFor(() -> assertThat(client.getBatches(), hasSize(1)));

        // to check that stream_timeout works we need to commit everything we consumed, in other case
        // Nakadi will first wait till commit_timeout exceeds
        final SubscriptionCursor lastBatchCursor = client.getBatches().get(client.getBatches().size() - 1).getCursor();
        commitCursors(subscription.getId(), ImmutableList.of(lastBatchCursor), client.getSessionId());

        waitFor(() -> assertThat(client.isRunning(), is(false)), 5000);
    }

    @Test(timeout = 10000)
    public void whenBatchLimitAndTimeoutAreSetTheyAreConsidered() throws Exception {

        range(0, 12).forEach(x -> publishEvent(eventType.getName(), "{\"foo\":\"bar\"}"));

        final TestStreamingClient client = TestStreamingClient
                .create(URL, subscription.getId(), "batch_limit=5&batch_flush_timeout=1&max_uncommitted_events=20")
                .start();

        waitFor(() -> assertThat(client.getBatches(), hasSize(3)));

        assertThat(client.getBatches().get(0).getEvents(), hasSize(5));
        assertThat(client.getBatches().get(0).getCursor().getOffset(), is("4"));

        assertThat(client.getBatches().get(1).getEvents(), hasSize(5));
        assertThat(client.getBatches().get(1).getCursor().getOffset(), is("9"));

        assertThat(client.getBatches().get(2).getEvents(), hasSize(2));
        assertThat(client.getBatches().get(2).getCursor().getOffset(), is("11"));
    }

    @Test(timeout = 10000)
    public void whenThereAreNoEmptySlotsThenConflict() throws Exception {

        final TestStreamingClient client = TestStreamingClient
                .create(URL, subscription.getId(), "batch_flush_timeout=1");
        client.start();
        waitFor(() -> assertThat(client.getBatches(), hasSize(1)));

        given()
                .get(format("/subscriptions/{0}/events", subscription.getId()))
                .then()
                .statusCode(SC_CONFLICT);
    }

    @Test(timeout = 10000)
    public void whenConnectionIsClosedByClientNakadiRecognizesIt() throws Exception {

        final TestStreamingClient client = TestStreamingClient
                .create(URL, subscription.getId(), "batch_flush_timeout=1");
        client.start();
        waitFor(() -> assertThat(client.getBatches(), hasSize(1)));

        client.close();
        Thread.sleep(2000);

        final TestStreamingClient anotherClient = TestStreamingClient
                .create(URL, subscription.getId(), "batch_flush_timeout=1");
        anotherClient.start();
        // if we start to get data for another client it means that Nakadi recognized that first client closed
        // connection (in other case it would not allow second client to connect because of lack of slots)
        waitFor(() -> assertThat(anotherClient.getBatches(), hasSize(1)));
    }

    @Test
    public void testGetSubscriptionStat() throws Exception {
        IntStream.range(0, 15).forEach(x -> publishEvent(eventType.getName(), "{\"foo\":\"bar\"}"));

        final TestStreamingClient client = TestStreamingClient
                .create(URL, subscription.getId(), "max_uncommitted_events=20")
                .start();
        waitFor(() -> assertThat(client.getBatches(), hasSize(15)));

        List<SubscriptionEventTypeStats> subscriptionStats =
                Collections.singletonList(new SubscriptionEventTypeStats(
                        eventType.getName(),
                        Collections.singleton(
                                new SubscriptionEventTypeStats.Partition("0", "assigned", 15L, client.getSessionId())))
                );
        NakadiTestUtils.getSubscriptionStat(subscription)
                .then()
                .content(new StringContains(JSON_TEST_HELPER.asJsonString(new ItemsWrapper<>(subscriptionStats))));

        final String partition = client.getBatches().get(0).getCursor().getPartition();
        final SubscriptionCursor cursor = new SubscriptionCursor(partition, "9", eventType.getName(), "token");
        commitCursors(subscription.getId(), ImmutableList.of(cursor), client.getSessionId());

        subscriptionStats =
                Collections.singletonList(new SubscriptionEventTypeStats(
                        eventType.getName(),
                        Collections.singleton(
                                new SubscriptionEventTypeStats.Partition("0", "assigned", 5L, client.getSessionId())))
                );
        NakadiTestUtils.getSubscriptionStat(subscription)
                .then()
                .content(new StringContains(JSON_TEST_HELPER.asJsonString(new ItemsWrapper<>(subscriptionStats))));
    }

    @Test(timeout = 10000)
    public void whenConsumerIsBlocked403() throws Exception {
        SettingsControllerAT.blacklist(eventType.getName(), BlacklistService.Type.CONSUMER_ET);

        final TestStreamingClient client1 = TestStreamingClient
                .create(URL, subscription.getId(), "")
                .start();
        waitFor(() -> Assert.assertEquals(403, client1.getResponseCode()));

        SettingsControllerAT.whitelist(eventType.getName(), BlacklistService.Type.CONSUMER_ET);

        final TestStreamingClient client2 = TestStreamingClient
                .create(URL, subscription.getId(), "")
                .start();
        waitFor(() -> Assert.assertEquals(HttpStatus.SC_OK, client2.getResponseCode()));
    }

    @Test(timeout = 10000)
    public void whenConsumerIsBlockedDuringConsumption() throws Exception {
        IntStream.range(0, 5).forEach(x -> publishEvent(eventType.getName(), "{\"foo\":\"bar\"}"));
        final TestStreamingClient client = TestStreamingClient
                .create(URL, subscription.getId(), "")
                .start();
        waitFor(() -> assertThat(client.getBatches(), hasSize(5)));
        SettingsControllerAT.blacklist(eventType.getName(), BlacklistService.Type.CONSUMER_ET);

        waitFor(() -> assertThat(client.getBatches(), hasSize(6)));

        Assert.assertEquals("Consumption is blocked",
                client.getBatches().get(client.getBatches().size() - 1).getMetadata().getDebug());
        SettingsControllerAT.whitelist(eventType.getName(), BlacklistService.Type.CONSUMER_ET);
    }

    @Test(timeout = 15000)
    public void whenStreamTimeout0ThenInfiniteStreaming() throws Exception {
        IntStream.range(0, 5).forEach(x -> publishEvent(eventType.getName(), "{\"foo\":\"bar\"}"));
        final TestStreamingClient client = TestStreamingClient
                .create(URL, subscription.getId(), "stream_timeout=0")
                .start();

        waitFor(() -> assertThat(client.getBatches(), hasSize(5)));
        Assert.assertFalse(client.getBatches().stream()
                .anyMatch(streamBatch -> streamBatch.getMetadata() != null
                        && streamBatch.getMetadata().getDebug().equals("Stream timeout reached")));
    }
}
