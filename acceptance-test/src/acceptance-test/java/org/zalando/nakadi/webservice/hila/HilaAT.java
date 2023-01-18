package org.zalando.nakadi.webservice.hila;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import org.apache.http.HttpStatus;
import org.hamcrest.Matchers;
import org.hamcrest.core.StringContains;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.zalando.nakadi.config.JsonConfig;
import org.zalando.nakadi.domain.EventType;
import org.zalando.nakadi.domain.ItemsWrapper;
import org.zalando.nakadi.domain.Subscription;
import org.zalando.nakadi.domain.SubscriptionBase;
import org.zalando.nakadi.domain.SubscriptionEventTypeStats;
import org.zalando.nakadi.service.BlacklistService;
import org.zalando.nakadi.util.ThreadUtils;
import org.zalando.nakadi.utils.JsonTestHelper;
import org.zalando.nakadi.utils.RandomSubscriptionBuilder;
import org.zalando.nakadi.view.Cursor;
import org.zalando.nakadi.view.EventTypePartitionView;
import org.zalando.nakadi.view.SubscriptionCursor;
import org.zalando.nakadi.view.SubscriptionCursorWithoutToken;
import org.zalando.nakadi.webservice.BaseAT;
import org.zalando.nakadi.webservice.SettingsControllerAT;
import org.zalando.nakadi.webservice.utils.NakadiTestUtils;
import org.zalando.nakadi.webservice.utils.TestStreamingClient;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static com.jayway.restassured.RestAssured.given;
import static com.jayway.restassured.RestAssured.when;
import static com.jayway.restassured.http.ContentType.JSON;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.zalando.nakadi.domain.SubscriptionBase.InitialPosition.BEGIN;
import static org.zalando.nakadi.domain.SubscriptionBase.InitialPosition.END;
import static org.zalando.nakadi.domain.SubscriptionEventTypeStats.Partition.AssignmentType.AUTO;
import static org.zalando.nakadi.domain.SubscriptionEventTypeStats.Partition.AssignmentType.DIRECT;
import static org.zalando.nakadi.utils.TestUtils.waitFor;
import static org.zalando.nakadi.webservice.hila.StreamBatch.MatcherIgnoringToken.equalToBatchIgnoringToken;
import static org.zalando.nakadi.webservice.hila.StreamBatch.singleEventBatch;
import static org.zalando.nakadi.webservice.utils.NakadiTestUtils.commitCursors;
import static org.zalando.nakadi.webservice.utils.NakadiTestUtils.createEventType;
import static org.zalando.nakadi.webservice.utils.NakadiTestUtils.createSubscription;
import static org.zalando.nakadi.webservice.utils.NakadiTestUtils.getNumberOfAssignedStreams;
import static org.zalando.nakadi.webservice.utils.NakadiTestUtils.publishEvent;
import static org.zalando.nakadi.webservice.utils.NakadiTestUtils.publishEvents;
import static org.zalando.nakadi.webservice.utils.TestStreamingClient.SESSION_ID_UNKNOWN;

public class HilaAT extends BaseAT {

    private static final ObjectMapper MAPPER = (new JsonConfig()).jacksonObjectMapper();
    private static final JsonTestHelper JSON_TEST_HELPER = new JsonTestHelper(MAPPER);
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

    @Test(timeout = 10000)
    public void whenStreamTimeoutReachedPossibleToCommit() throws Exception {
        final TestStreamingClient client = TestStreamingClient
                .create(URL, subscription.getId(), "batch_limit=1&stream_limit=2&stream_timeout=1")
                .start();
        waitFor(() -> assertThat(client.getSessionId(), Matchers.not(equalTo(SESSION_ID_UNKNOWN))));

        publishEvent(eventType.getName(), "{\"foo\":\"bar\"}");
        waitFor(() -> Assert.assertFalse(client.getJsonBatches().isEmpty()), TimeUnit.SECONDS.toMillis(2), 100);
        final SubscriptionCursor toCommit = client.getJsonBatches().get(0).getCursor();
        client.close(); // connection is closed, and stream as well
        ThreadUtils.sleep(TimeUnit.SECONDS.toMillis(1));
        final int statusCode = commitCursors(
                subscription.getId(),
                Collections.singletonList(toCommit),
                client.getSessionId());
        Assert.assertEquals(HttpStatus.SC_NO_CONTENT, statusCode);
    }

    @Test(timeout = 30000)
    public void whenEventTypeRepartitionedTheNewSubscriptionShouldHaveUpdatedPartition() throws Exception {
        final EventType eventType = NakadiTestUtils.createBusinessEventTypeWithPartitions(1);
        NakadiTestUtils.publishBusinessEventWithUserDefinedPartition(
                eventType.getName(), 1, x -> "{\"foo\":\"bar\"}", p -> "0");

        NakadiTestUtils.repartitionEventType(eventType, 2);
        Thread.sleep(1500);

        final Subscription subscription = createSubscription(
                RandomSubscriptionBuilder.builder()
                        .withEventType(eventType.getName())
                        .withStartFrom(BEGIN)
                        .buildSubscriptionBase());

        final TestStreamingClient clientAfterRepartitioning = TestStreamingClient
                .create(URL, subscription.getId(), "")
                .start();

        NakadiTestUtils.publishBusinessEventWithUserDefinedPartition(
                eventType.getName(), 1, x -> "{\"foo\":\"bar" + x + "\"}", p -> "1");

        waitFor(() -> assertThat(clientAfterRepartitioning.getJsonBatches(), Matchers.hasSize(2)));

        Assert.assertTrue(clientAfterRepartitioning.getJsonBatches().stream()
                .anyMatch(batch -> batch.getCursor().getPartition().equals("1")));
    }

    @Test(timeout = 10000)
    public void whenStreamTimeoutReachedThenEventsFlushed() {
        final TestStreamingClient client = TestStreamingClient
                .create(URL, subscription.getId(),
                        "batch_flush_timeout=600&batch_limit=1000&stream_timeout=2&max_uncommitted_events=1000")
                .start();
        waitFor(() -> assertThat(client.getSessionId(), Matchers.not(equalTo(SESSION_ID_UNKNOWN))));

        publishEvents(eventType.getName(), 4, x -> "{\"foo\":\"bar\"}");

        // when stream_timeout is reached we should get 2 batches:
        // first one containing 4 events, second one with debug message
        waitFor(() -> assertThat(client.getJsonBatches(), Matchers.hasSize(2)));
        assertThat(client.getJsonBatches().get(0).getEvents(), Matchers.hasSize(4));
        assertThat(client.getJsonBatches().get(1).getEvents(), Matchers.hasSize(0));
        System.out.println(client.getJsonBatches());
    }

    @Test(timeout = 30000)
    public void whenOffsetIsCommittedNextSessionStartsFromNextEventAfterCommitted() throws Exception {
        // write 4 events to event-type
        publishEvents(eventType.getName(), 4, x -> "{\"foo\":\"bar" + x + "\"}");

        // create session, read from subscription and wait for events to be sent
        final TestStreamingClient client = TestStreamingClient
                .create(URL, subscription.getId(), "stream_limit=2")
                .start();
        waitFor(() -> assertThat(client.getJsonBatches(), Matchers.hasSize(2)));
        assertThat(client.getJsonBatches().get(0), equalToBatchIgnoringToken(singleEventBatch("0",
                "001-0001-000000000000000000", eventType.getName(), ImmutableMap.of("foo", "bar0"),
                "Stream started")));
        assertThat(client.getJsonBatches().get(1), equalToBatchIgnoringToken(singleEventBatch("0",
                "001-0001-000000000000000001", eventType.getName(), ImmutableMap.of("foo", "bar1"))));

        // commit offset that will also trigger session closing as we reached stream_limit and committed
        commitCursors(subscription.getId(), ImmutableList.of(client.getJsonBatches().get(1).getCursor()),
                client.getSessionId());
        waitFor(() -> assertThat(client.isRunning(), is(false)));

        // create new session and read from subscription again
        client.start();
        waitFor(() -> assertThat(client.getJsonBatches(), Matchers.hasSize(2)));

        // check that we have read the next two events with correct offsets
        assertThat(client.getJsonBatches().get(0), equalToBatchIgnoringToken(singleEventBatch("0",
                "001-0001-000000000000000002", eventType.getName(),
                ImmutableMap.of("foo", "bar2"), "Stream started")));
        assertThat(client.getJsonBatches().get(1), equalToBatchIgnoringToken(singleEventBatch("0",
                "001-0001-000000000000000003", eventType.getName(), ImmutableMap.of("foo", "bar3"))));
    }


    @Test(timeout = 5000)
    public void whenNoEventsThenFirstOffsetIsBEGIN() {
        final TestStreamingClient client = TestStreamingClient
                .create(URL, subscription.getId(), "batch_flush_timeout=1")
                .start();
        waitFor(() -> assertThat(client.getJsonBatches(), Matchers.not(Matchers.empty())));
        assertThat(client.getJsonBatches().get(0).getCursor().getOffset(), equalTo("001-0001--1"));
    }

    @Test(timeout = 5000)
    public void whenNoEventsThenBeginOffsetIsUsed() throws Exception {
        final TestStreamingClient client = TestStreamingClient
                .create(subscription.getId())
                .start();
        waitFor(() -> assertThat(client.getSessionId(), Matchers.not(equalTo(SESSION_ID_UNKNOWN))));

        when().get("/subscriptions/{sid}/cursors", subscription.getId())
                .then()
                .body("items[0].offset", equalTo("001-0001--1"));

        final int commitResult = commitCursors(subscription.getId(),
                ImmutableList.of(new SubscriptionCursor("0", Cursor.BEFORE_OLDEST_OFFSET, eventType.getName(), "abc")),
                client.getSessionId());
        assertThat(commitResult, equalTo(HttpStatus.SC_OK));
    }

    @Test(timeout = 5000)
    public void whenCommitVeryFirstEventThenOk() throws Exception {
        publishEvent(eventType.getName(), "{\"foo\":\"bar\"}");

        // create session, read from subscription and wait for events to be sent
        final TestStreamingClient client = TestStreamingClient
                .create(subscription.getId())
                .start();
        waitFor(() -> assertThat(client.getJsonBatches(), Matchers.not(Matchers.empty())));

        // commit and check that status is 204
        final int commitResult = commitCursors(subscription.getId(),
                ImmutableList.of(new SubscriptionCursor("0", "0", eventType.getName(), "token")),
                client.getSessionId());
        assertThat(commitResult, equalTo(HttpStatus.SC_NO_CONTENT));
    }

    @Test(timeout = 15000)
    public void whenWindowSizeIsSetItIsConsidered() throws Exception {

        publishEvents(eventType.getName(), 15, i -> "{\"foo\":\"bar\"}");

        final TestStreamingClient client = TestStreamingClient
                .create(URL, subscription.getId(), "max_uncommitted_events=5")
                .start();

        waitFor(() -> assertThat(client.getJsonBatches(), Matchers.hasSize(5)));

        SubscriptionCursor cursorToCommit = client.getJsonBatches().get(4).getCursor();
        commitCursors(subscription.getId(), ImmutableList.of(cursorToCommit), client.getSessionId());

        waitFor(() -> assertThat(client.getJsonBatches(), Matchers.hasSize(10)));

        cursorToCommit = client.getJsonBatches().get(6).getCursor();
        commitCursors(subscription.getId(), ImmutableList.of(cursorToCommit), client.getSessionId());

        waitFor(() -> assertThat(client.getJsonBatches(), Matchers.hasSize(12)));
    }

    @Test(timeout = 15000)
    public void whenCommitTimeoutReachedSessionIsClosed() {

        publishEvent(eventType.getName(), "{\"foo\":\"bar\"}");

        final TestStreamingClient client = TestStreamingClient
                .create(subscription.getId()) // commit_timeout is 5 seconds for test
                .start();

        waitFor(() -> assertThat(client.getJsonBatches(), Matchers.hasSize(2)), 10000);
        waitFor(() -> assertThat(client.isRunning(), is(false)), 10000);
        assertThat(client.getJsonBatches().get(1), equalToBatchIgnoringToken(singleEventBatch("0",
                "001-0001-000000000000000000", eventType.getName(), ImmutableMap.of(), "Commit timeout reached")));
    }

    @Test(timeout = 15000)
    public void whenStreamTimeoutReachedSessionIsClosed() throws Exception {

        publishEvent(eventType.getName(), "{\"foo\":\"bar\"}");

        final TestStreamingClient client = TestStreamingClient
                .create(URL, subscription.getId(), "stream_timeout=3")
                .start();

        waitFor(() -> assertThat(client.getJsonBatches(), Matchers.hasSize(1)));

        // to check that stream_timeout works we need to commit everything we consumed, in other case
        // Nakadi will first wait till commit_timeout exceeds
        final SubscriptionCursor lastBatchCursor = client.getJsonBatches()
                .get(client.getJsonBatches().size() - 1).getCursor();
        commitCursors(subscription.getId(), ImmutableList.of(lastBatchCursor), client.getSessionId());

        waitFor(() -> assertThat(client.isRunning(), is(false)), 5000);
    }

    @Test(timeout = 10000)
    public void whenBatchLimitAndTimeoutAreSetTheyAreConsidered() {

        publishEvents(eventType.getName(), 12, i -> "{\"foo\":\"bar\"}");

        final TestStreamingClient client = TestStreamingClient
                .create(URL, subscription.getId(), "batch_limit=5&batch_flush_timeout=1&max_uncommitted_events=20")
                .start();

        waitFor(() -> assertThat(client.getJsonBatches(), Matchers.hasSize(3)));

        assertThat(client.getJsonBatches().get(0).getEvents(), Matchers.hasSize(5));
        assertThat(client.getJsonBatches().get(0).getCursor().getOffset(), is("001-0001-000000000000000004"));

        assertThat(client.getJsonBatches().get(1).getEvents(), Matchers.hasSize(5));
        assertThat(client.getJsonBatches().get(1).getCursor().getOffset(), is("001-0001-000000000000000009"));

        assertThat(client.getJsonBatches().get(2).getEvents(), Matchers.hasSize(2));
        assertThat(client.getJsonBatches().get(2).getCursor().getOffset(), is("001-0001-000000000000000011"));
    }

    @Test(timeout = 10000)
    public void whenThereAreNoEmptySlotsThenConflict() {

        final TestStreamingClient client = TestStreamingClient
                .create(URL, subscription.getId(), "batch_flush_timeout=1");
        client.start();
        waitFor(() -> assertThat(client.getJsonBatches(), Matchers.hasSize(1)));

        given()
                .get("/subscriptions/{id}/events", subscription.getId())
                .then()
                .statusCode(HttpStatus.SC_CONFLICT);
    }

    @Test(timeout = 30000)
    public void whenConnectionIsClosedByClientNakadiRecognizesIt() throws Exception {

        final TestStreamingClient client = TestStreamingClient
                .create(URL, subscription.getId(), "batch_flush_timeout=1");
        client.start();
        waitFor(() -> assertThat(client.getJsonBatches(), Matchers.hasSize(1)));

        client.close();
        ThreadUtils.sleep(10000);

        final TestStreamingClient anotherClient = TestStreamingClient
                .create(URL, subscription.getId(), "batch_flush_timeout=1");
        anotherClient.start();
        // if we start to get data for another client it means that Nakadi recognized that first client closed
        // connection (in other case it would not allow second client to connect because of lack of slots)
        waitFor(() -> assertThat(anotherClient.getJsonBatches(), Matchers.hasSize(1)));
    }

    @Test(timeout = 10000)
    public void testGetSubscriptionStat() throws Exception {
        publishEvents(eventType.getName(), 15, i -> "{\"foo\":\"bar\"}");

        final TestStreamingClient client = TestStreamingClient
                .create(URL, subscription.getId(), "max_uncommitted_events=20")
                .start();
        waitFor(() -> assertThat(client.getJsonBatches(), Matchers.hasSize(15)));

        List<SubscriptionEventTypeStats> subscriptionStats =
                Collections.singletonList(new SubscriptionEventTypeStats(
                        eventType.getName(),
                        Collections.singletonList(new SubscriptionEventTypeStats.Partition(
                                "0",
                                "assigned",
                                15L,
                                null,
                                client.getSessionId(),
                                AUTO)))
                );
        NakadiTestUtils.getSubscriptionStat(subscription)
                .then()
                .content(new StringContains(JSON_TEST_HELPER.asJsonString(new ItemsWrapper<>(subscriptionStats))));

        final String partition = client.getJsonBatches().get(0).getCursor().getPartition();
        final SubscriptionCursor cursor = new SubscriptionCursor(partition, "9", eventType.getName(), "token");
        commitCursors(subscription.getId(), ImmutableList.of(cursor), client.getSessionId());

        subscriptionStats =
                Collections.singletonList(new SubscriptionEventTypeStats(
                        eventType.getName(),
                        Collections.singletonList(new SubscriptionEventTypeStats.Partition(
                                "0",
                                "assigned",
                                5L,
                                null,
                                client.getSessionId(),
                                AUTO)))
                );
        NakadiTestUtils.getSubscriptionStat(subscription)
                .then()
                .content(new StringContains(JSON_TEST_HELPER.asJsonString(new ItemsWrapper<>(subscriptionStats))));
    }

    @Test(timeout = 10000)
    public void testGetSubscriptionStatWhenDirectAssignment() throws Exception {
        // connect with 1 stream directly requesting the partition
        final TestStreamingClient client = new TestStreamingClient(URL, subscription.getId(), "",
                Optional.empty(),
                Optional.of("{\"partitions\":[" +
                        "{\"event_type\":\"" + eventType.getName() + "\",\"partition\":\"0\"}]}"));
        client.start();
        // wait for rebalance to finish
        waitFor(() -> assertThat(getNumberOfAssignedStreams(subscription.getId()), Matchers.is(1)));

        NakadiTestUtils.getSubscriptionStat(subscription)
                .then()
                .content(new StringContains(JSON_TEST_HELPER.asJsonString(new SubscriptionEventTypeStats(
                        eventType.getName(),
                        Collections.singletonList(new SubscriptionEventTypeStats.Partition(
                                "0",
                                "assigned",
                                0L,
                                null,
                                client.getSessionId(),
                                DIRECT
                        ))))));
    }

    @Test
    public void testSubscriptionStatsMultiET() throws IOException {
        final List<EventType> eventTypes = Lists.newArrayList(createEventType(), createEventType());
        publishEvents(eventTypes.get(0).getName(), 10, i -> "{\"foo\":\"bar\"}");
        publishEvents(eventTypes.get(1).getName(), 20, i -> "{\"foo\":\"bar\"}");

        final Subscription subscription = createSubscription(RandomSubscriptionBuilder.builder()
                .withEventTypes(eventTypes.stream().map(EventType::getName).collect(Collectors.toSet()))
                .withStartFrom(END)
                .build());
        // client is needed only to initialize stats
        final TestStreamingClient client = TestStreamingClient
                .create(URL, subscription.getId(), "batch_flush_timeout=1")
                .start();

        waitFor(() -> assertThat(client.getJsonBatches().isEmpty(), is(false)));

        publishEvents(eventTypes.get(0).getName(), 1, i -> "{\"foo\":\"bar\"}");
        publishEvents(eventTypes.get(1).getName(), 2, i -> "{\"foo\":\"bar\"}");

        NakadiTestUtils.getSubscriptionStat(subscription)
                .then()
                .content(new StringContains(JSON_TEST_HELPER.asJsonString(new SubscriptionEventTypeStats(
                        eventTypes.get(0).getName(),
                        Collections.singletonList(new SubscriptionEventTypeStats.Partition(
                                "0",
                                "assigned",
                                1L,
                                null,
                                client.getSessionId(),
                                AUTO
                        ))))))
                .content(new StringContains(JSON_TEST_HELPER.asJsonString(new SubscriptionEventTypeStats(
                        eventTypes.get(1).getName(),
                        Collections.singletonList(new SubscriptionEventTypeStats.Partition(
                                "0",
                                "assigned",
                                2L,
                                null,
                                client.getSessionId(),
                                AUTO
                        ))))));
        client.close();
    }

    @Test(timeout = 10000)
    public void whenConsumerIsBlocked403() throws Exception {
        SettingsControllerAT.blacklist(eventType.getName(), BlacklistService.Type.CONSUMER_ET);

        final TestStreamingClient client1 = TestStreamingClient
                .create(subscription.getId())
                .start();
        waitFor(() -> Assert.assertEquals(403, client1.getResponseCode()));

        SettingsControllerAT.whitelist(eventType.getName(), BlacklistService.Type.CONSUMER_ET);

        final TestStreamingClient client2 = TestStreamingClient
                .create(subscription.getId())
                .start();
        waitFor(() -> Assert.assertEquals(HttpStatus.SC_OK, client2.getResponseCode()));
    }

    @Test(timeout = 10000)
    public void whenConsumerIsBlockedDuringConsumption() throws Exception {
        publishEvents(eventType.getName(), 5, i -> "{\"foo\":\"bar\"}");
        final TestStreamingClient client = TestStreamingClient
                .create(subscription.getId())
                .start();
        waitFor(() -> assertThat(client.getJsonBatches(), Matchers.hasSize(5)));
        SettingsControllerAT.blacklist(eventType.getName(), BlacklistService.Type.CONSUMER_ET);

        waitFor(() -> assertThat(client.getJsonBatches(), Matchers.hasSize(6)));

        Assert.assertEquals("Consumption is blocked",
                client.getJsonBatches().get(client.getJsonBatches().size() - 1).getMetadata().getDebug());
        SettingsControllerAT.whitelist(eventType.getName(), BlacklistService.Type.CONSUMER_ET);
    }

    @Test(timeout = 15000)
    public void whenStreamTimeout0ThenInfiniteStreaming() {
        publishEvents(eventType.getName(), 5, i -> "{\"foo\":\"bar\"}");
        final TestStreamingClient client = TestStreamingClient
                .create(URL, subscription.getId(), "stream_timeout=0")
                .start();

        waitFor(() -> assertThat(client.getJsonBatches(), Matchers.hasSize(5)));
        Assert.assertFalse(client.getJsonBatches().stream()
                .anyMatch(streamBatch -> streamBatch.getMetadata() != null
                        && streamBatch.getMetadata().getDebug().equals("Stream timeout reached")));
    }

    @Test(timeout = 15000)
    public void whenResetCursorsThenStreamFromResetCursorOffset() throws Exception {
        publishEvents(eventType.getName(), 20, i -> "{\"foo\":\"bar\"}");
        final TestStreamingClient client1 = TestStreamingClient
                .create(subscription.getId())
                .start();
        waitFor(() -> assertThat(client1.getJsonBatches(), Matchers.hasSize(10)));

        int statusCode = commitCursors(
                subscription.getId(),
                Collections.singletonList(client1.getJsonBatches().get(9).getCursor()),
                client1.getSessionId());
        Assert.assertEquals(HttpStatus.SC_NO_CONTENT, statusCode);

        final List<SubscriptionCursor> resetCursors =
                Collections.singletonList(client1.getJsonBatches().get(4).getCursor());
        statusCode = given()
                .body(MAPPER.writeValueAsString(new ItemsWrapper<>(resetCursors)))
                .contentType(JSON)
                .patch("/subscriptions/{id}/cursors", subscription.getId())
                .getStatusCode();
        Assert.assertEquals(HttpStatus.SC_NO_CONTENT, statusCode);
        Assert.assertFalse(client1.isRunning());
        Assert.assertTrue(client1.getJsonBatches().stream()
                .anyMatch(streamBatch -> streamBatch.getMetadata() != null
                        && streamBatch.getMetadata().getDebug().equals("Resetting subscription cursors")));


        final TestStreamingClient client2 = TestStreamingClient
                .create(subscription.getId())
                .start();
        waitFor(() -> assertThat(client2.getJsonBatches(), Matchers.hasSize(10)));

        Assert.assertEquals("001-0001-000000000000000005", client2.getJsonBatches().get(0).getCursor().getOffset());
    }

    @Test(timeout = 15000)
    public void whenPatchThenCursorsAreInitializedToDefault() throws Exception {
        final EventType et = createEventType();
        publishEvents(et.getName(), 10, i -> "{\"foo\": \"bar\"}");
        ThreadUtils.sleep(1000L);
        final Subscription s = createSubscription(RandomSubscriptionBuilder.builder()
                .withEventType(et.getName())
                .withStartFrom(END)
                .buildSubscriptionBase());
        given()
                .body(MAPPER.writeValueAsString(new ItemsWrapper<>(Collections.emptyList())))
                .contentType(JSON)
                .patch("/subscriptions/{id}/cursors", s.getId())
                .then()
                .statusCode(HttpStatus.SC_NO_CONTENT);

        final ItemsWrapper<SubscriptionCursor> subscriptionCursors = MAPPER.readValue(
                given().get("/subscriptions/{id}/cursors", s.getId()).getBody().asString(),
                new TypeReference<ItemsWrapper<SubscriptionCursor>>() {
                }
        );
        final List<EventTypePartitionView> etStats = MAPPER.readValue(
                given().get("/event-types/{et}/partitions", et.getName()).getBody().asString(),
                new TypeReference<List<EventTypePartitionView>>() {
                }
        );
        Assert.assertEquals(subscriptionCursors.getItems().size(), etStats.size());
        subscriptionCursors.getItems().forEach(sCursor -> {
            final boolean offsetSame = etStats.stream()
                    .anyMatch(ss -> ss.getPartitionId().equals(sCursor.getPartition()) &&
                            ss.getNewestAvailableOffset().equals(sCursor.getOffset()));
            // Check that after patch cursors are the same as END
            Assert.assertTrue(offsetSame);
        });
    }

    @Test(timeout = 15000)
    public void whenPatchThenCursorsAreInitializedAndPatched() throws Exception {
        final EventType et = createEventType();
        publishEvents(et.getName(), 10, i -> "{\"foo\": \"bar\"}");
        final List<EventTypePartitionView> etStats = MAPPER.readValue(
                given().get("/event-types/{et}/partitions", et.getName()).getBody().asString(),
                new TypeReference<List<EventTypePartitionView>>() {
                }
        );
        final EventTypePartitionView begin = etStats.get(0);
        final Subscription s = createSubscription(RandomSubscriptionBuilder.builder()
                .withEventType(et.getName())
                .withStartFrom(END)
                .buildSubscriptionBase());
        given()
                .body(MAPPER.writeValueAsString(new ItemsWrapper<>(Collections.singletonList(
                        new SubscriptionCursorWithoutToken(
                                et.getName(), begin.getPartitionId(), begin.getOldestAvailableOffset())
                ))))
                .contentType(JSON)
                .patch("/subscriptions/{id}/cursors", s.getId())
                .then()
                .statusCode(HttpStatus.SC_NO_CONTENT);

        final ItemsWrapper<SubscriptionCursor> subscriptionCursors = MAPPER.readValue(
                given().get("/subscriptions/{id}/cursors", s.getId()).getBody().asString(),
                new TypeReference<ItemsWrapper<SubscriptionCursor>>() {
                }
        );

        Assert.assertEquals(subscriptionCursors.getItems().size(), etStats.size());

        subscriptionCursors.getItems().forEach(item -> {
            if (item.getPartition().equals(begin.getPartitionId())) {
                Assert.assertEquals(begin.getOldestAvailableOffset(), item.getOffset());
            } else {
                Assert.assertEquals(begin.getNewestAvailableOffset(), item.getOffset());
            }
        });

    }
}
