package org.zalando.nakadi.webservice.hila;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.jayway.restassured.response.Response;
import org.apache.http.HttpStatus;
import org.json.JSONObject;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.zalando.nakadi.config.JsonConfig;
import org.zalando.nakadi.domain.EventType;
import org.zalando.nakadi.domain.ItemsWrapper;
import org.zalando.nakadi.domain.Subscription;
import org.zalando.nakadi.domain.SubscriptionBase;
import org.zalando.nakadi.domain.SubscriptionEventTypeStats;
import org.zalando.nakadi.domain.UnprocessableEventPolicy;
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
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static com.jayway.restassured.RestAssured.given;
import static com.jayway.restassured.RestAssured.when;
import static com.jayway.restassured.http.ContentType.JSON;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.zalando.nakadi.annotations.validation.DeadLetterAnnotationValidator.SUBSCRIPTION_MAX_EVENT_SEND_COUNT;
import static org.zalando.nakadi.annotations.validation.DeadLetterAnnotationValidator.
    SUBSCRIPTION_UNPROCESSABLE_EVENT_POLICY;
import static org.zalando.nakadi.domain.SubscriptionBase.InitialPosition.BEGIN;
import static org.zalando.nakadi.domain.SubscriptionBase.InitialPosition.END;
import static org.zalando.nakadi.domain.SubscriptionEventTypeStats.Partition.AssignmentType.AUTO;
import static org.zalando.nakadi.domain.SubscriptionEventTypeStats.Partition.AssignmentType.DIRECT;
import static org.zalando.nakadi.utils.TestUtils.waitFor;
import static org.zalando.nakadi.webservice.utils.NakadiTestUtils.commitCursors;
import static org.zalando.nakadi.webservice.utils.NakadiTestUtils.createEventType;
import static org.zalando.nakadi.webservice.utils.NakadiTestUtils.createSubscription;
import static org.zalando.nakadi.webservice.utils.NakadiTestUtils.createSubscriptionForEventType;
import static org.zalando.nakadi.webservice.utils.NakadiTestUtils.getNumberOfAssignedStreams;
import static org.zalando.nakadi.webservice.utils.NakadiTestUtils.publishBusinessEventWithUserDefinedPartition;
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
        waitFor(() -> assertThat(client.getSessionId(), not(equalTo(SESSION_ID_UNKNOWN))));

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
        publishBusinessEventWithUserDefinedPartition(
                eventType.getName(), 1, x -> "{\"foo\":\"bar\"}", p -> "0");
        NakadiTestUtils.repartitionEventType(eventType, 2);
        final Subscription subscription = createSubscription(
                RandomSubscriptionBuilder.builder()
                        .withEventType(eventType.getName())
                        .withStartFrom(BEGIN)
                        .buildSubscriptionBase());
        final TestStreamingClient clientAfterRepartitioning = TestStreamingClient
                .create(URL, subscription.getId(), "")
                .start();
        publishBusinessEventWithUserDefinedPartition(
                eventType.getName(), 1, x -> "{\"foo\":\"bar" + x + "\"}", p -> "1");
        waitFor(() -> assertThat(clientAfterRepartitioning.getJsonBatches(), hasSize(2)));
        Assert.assertTrue(clientAfterRepartitioning.getJsonBatches().stream()
                .anyMatch(event -> event.getCursor().getPartition().equals("1")));
    }

    @Test(timeout = 10000)
    public void whenStreamTimeoutReachedThenEventsFlushed() {
        final TestStreamingClient client = TestStreamingClient
                .create(URL, subscription.getId(),
                        "batch_flush_timeout=600&batch_limit=1000&stream_timeout=2&max_uncommitted_events=1000")
                .start();
        waitFor(() -> assertThat(client.getSessionId(), not(equalTo(SESSION_ID_UNKNOWN))));

        publishEvents(eventType.getName(), 4, x -> "{\"foo\":\"bar\"}");

        // when stream_timeout is reached we should get 2 batches:
        // first one containing 4 events, second one with debug message
        waitFor(() -> assertThat(client.getJsonBatches(), hasSize(2)));
        assertThat(client.getJsonBatches().get(0).getEvents(), hasSize(4));
        assertThat(client.getJsonBatches().get(1).getEvents(), hasSize(0));
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
        waitFor(() -> assertThat(client.getJsonBatches(), hasSize(2)));
        assertThat(
                client.getJsonBatches().get(0),
                StreamBatch.equalToBatchIgnoringToken(
                        StreamBatch.singleEventBatch("0", "001-0001-000000000000000000", eventType.getName(),
                                new JSONObject().put("foo", "bar0"), "Stream started")));
        assertThat(
                client.getJsonBatches().get(1),
                StreamBatch.equalToBatchIgnoringToken(
                        StreamBatch.singleEventBatch("0", "001-0001-000000000000000001", eventType.getName(),
                                new JSONObject().put("foo", "bar1"))));

        // commit offset that will also trigger session closing as we reached stream_limit and committed
        commitCursors(subscription.getId(), ImmutableList.of(client.getJsonBatches().get(1).getCursor()),
                client.getSessionId());
        waitFor(() -> assertThat(client.isRunning(), is(false)));

        // create new session and read from subscription again
        client.start();
        waitFor(() -> assertThat(client.getJsonBatches(), hasSize(2)));

        // check that we have read the next two events with correct offsets
        assertThat(
                client.getJsonBatches().get(0),
                StreamBatch.equalToBatchIgnoringToken(
                        StreamBatch.singleEventBatch("0", "001-0001-000000000000000002", eventType.getName(),
                                new JSONObject().put("foo", "bar2"), "Stream started")));
        assertThat(
                client.getJsonBatches().get(1),
                StreamBatch.equalToBatchIgnoringToken(
                        StreamBatch.singleEventBatch("0", "001-0001-000000000000000003", eventType.getName(),
                                new JSONObject().put("foo", "bar3"))));
    }

    @Test(timeout = 5000)
    public void whenNoEventsThenFirstOffsetIsBEGIN() {
        final TestStreamingClient client = TestStreamingClient
                .create(URL, subscription.getId(), "batch_flush_timeout=1")
                .start();
        waitFor(() -> assertThat(client.getJsonBatches(), not(empty())));
        assertThat(client.getJsonBatches().get(0).getCursor().getOffset(), equalTo("001-0001--1"));
    }

    @Test(timeout = 5000)
    public void whenNoEventsThenBeginOffsetIsUsed() throws Exception {
        final TestStreamingClient client = TestStreamingClient
                .create(subscription.getId())
                .start();
        waitFor(() -> assertThat(client.getSessionId(), not(equalTo(SESSION_ID_UNKNOWN))));

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
        waitFor(() -> assertThat(client.getJsonBatches(), not(empty())));

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

        waitFor(() -> assertThat(client.getJsonBatches(), hasSize(5)));

        SubscriptionCursor cursorToCommit = client.getJsonBatches().get(4).getCursor();
        commitCursors(subscription.getId(), ImmutableList.of(cursorToCommit), client.getSessionId());

        waitFor(() -> assertThat(client.getJsonBatches(), hasSize(10)));

        cursorToCommit = client.getJsonBatches().get(6).getCursor();
        commitCursors(subscription.getId(), ImmutableList.of(cursorToCommit), client.getSessionId());

        waitFor(() -> assertThat(client.getJsonBatches(), hasSize(12)));
    }

    @Test(timeout = 15000)
    public void whenCommitTimeoutReachedSessionIsClosed() {

        publishEvent(eventType.getName(), "{\"foo\":\"bar\"}");

        final TestStreamingClient client = TestStreamingClient
                .create(subscription.getId()) // commit_timeout is 5 seconds for test
                .start();

        waitFor(() -> assertThat(client.getJsonBatches(), hasSize(2)), 10000);
        waitFor(() -> assertThat(client.isRunning(), is(false)), 10000);
        assertThat(
                client.getJsonBatches().get(1),
                StreamBatch.equalToBatchIgnoringToken(
                        StreamBatch.emptyBatch("0", "001-0001-000000000000000000", eventType.getName(),
                                "Commit timeout reached")));
    }

    @Test(timeout = 15000)
    public void whenStreamTimeoutReachedSessionIsClosed() throws Exception {

        publishEvent(eventType.getName(), "{\"foo\":\"bar\"}");

        final TestStreamingClient client = TestStreamingClient
                .create(URL, subscription.getId(), "stream_timeout=3")
                .start();

        waitFor(() -> assertThat(client.getJsonBatches(), hasSize(1)));

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

        waitFor(() -> assertThat(client.getJsonBatches(), hasSize(3)));

        assertThat(client.getJsonBatches().get(0).getEvents(), hasSize(5));
        assertThat(client.getJsonBatches().get(0).getCursor().getOffset(), is("001-0001-000000000000000004"));

        assertThat(client.getJsonBatches().get(1).getEvents(), hasSize(5));
        assertThat(client.getJsonBatches().get(1).getCursor().getOffset(), is("001-0001-000000000000000009"));

        assertThat(client.getJsonBatches().get(2).getEvents(), hasSize(2));
        assertThat(client.getJsonBatches().get(2).getCursor().getOffset(), is("001-0001-000000000000000011"));
    }

    @Test(timeout = 10000)
    public void whenThereAreNoEmptySlotsThenConflict() {

        final TestStreamingClient client = TestStreamingClient
                .create(URL, subscription.getId(), "batch_flush_timeout=1");
        client.start();
        waitFor(() -> assertThat(client.getJsonBatches(), hasSize(1)));

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
        waitFor(() -> assertThat(client.getJsonBatches(), hasSize(1)));

        client.close();
        ThreadUtils.sleep(10000);

        final TestStreamingClient anotherClient = TestStreamingClient
                .create(URL, subscription.getId(), "batch_flush_timeout=1");
        anotherClient.start();
        // if we start to get data for another client it means that Nakadi recognized that first client closed
        // connection (in other case it would not allow second client to connect because of lack of slots)
        waitFor(() -> assertThat(anotherClient.getJsonBatches(), hasSize(1)));
    }

    @Test(timeout = 10000)
    public void testGetSubscriptionStat() throws Exception {
        publishEvents(eventType.getName(), 15, i -> "{\"foo\":\"bar\"}");

        final TestStreamingClient client = TestStreamingClient
                .create(URL, subscription.getId(), "max_uncommitted_events=20")
                .start();
        waitFor(() -> assertThat(client.getJsonBatches(), hasSize(15)));

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
                .content(containsString(JSON_TEST_HELPER.asJsonString(new ItemsWrapper<>(subscriptionStats))));

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
                .content(containsString(JSON_TEST_HELPER.asJsonString(new ItemsWrapper<>(subscriptionStats))));
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
        waitFor(() -> assertThat(getNumberOfAssignedStreams(subscription.getId()), is(1)));

        NakadiTestUtils.getSubscriptionStat(subscription)
                .then()
                .content(containsString(JSON_TEST_HELPER.asJsonString(new SubscriptionEventTypeStats(
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
                .content(containsString(JSON_TEST_HELPER.asJsonString(new SubscriptionEventTypeStats(
                        eventTypes.get(0).getName(),
                        Collections.singletonList(new SubscriptionEventTypeStats.Partition(
                                "0",
                                "assigned",
                                1L,
                                null,
                                client.getSessionId(),
                                AUTO
                        ))))))
                .content(containsString(JSON_TEST_HELPER.asJsonString(new SubscriptionEventTypeStats(
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
        waitFor(() -> assertThat(client.getJsonBatches(), hasSize(5)));
        SettingsControllerAT.blacklist(eventType.getName(), BlacklistService.Type.CONSUMER_ET);

        waitFor(() -> assertThat(client.getJsonBatches(), hasSize(6)));

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

        waitFor(() -> assertThat(client.getJsonBatches(), hasSize(5)));
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
        waitFor(() -> assertThat(client1.getJsonBatches(), hasSize(10)));

        final int statusCode = commitCursors(
                subscription.getId(),
                Collections.singletonList(client1.getJsonBatches().get(9).getCursor()),
                client1.getSessionId());
        Assert.assertEquals(HttpStatus.SC_NO_CONTENT, statusCode);

        final List<SubscriptionCursor> cursorsToReset =
                Collections.singletonList(client1.getJsonBatches().get(4).getCursor());
        resetCursorsWithToken(subscription.getId(), cursorsToReset);
        Assert.assertFalse(client1.isRunning());
        Assert.assertTrue(client1.getJsonBatches().stream()
                .anyMatch(streamBatch -> streamBatch.getMetadata() != null
                        && streamBatch.getMetadata().getDebug().equals("Resetting subscription cursors")));


        final TestStreamingClient client2 = TestStreamingClient
                .create(subscription.getId())
                .start();
        waitFor(() -> assertThat(client2.getJsonBatches(), hasSize(10)));

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
        resetCursors(s.getId(), List.of());

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
        resetCursors(s.getId(), Collections.singletonList(
                new SubscriptionCursorWithoutToken(
                        et.getName(), begin.getPartitionId(), begin.getOldestAvailableOffset())
        ));

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

    @Test(timeout = 15000)
    public void whenCommitFailsThreeTimesAndSingleBatchEventFailsThreeTimesThenEventSkipped() throws IOException {
        publishEvents(eventType.getName(), 50, i -> "{\"foo\":\"bar\"}");

        final Subscription subscription = createAutoDLQSubscription(eventType, UnprocessableEventPolicy.SKIP_EVENT, 3);
        final TestStreamingClient client = TestStreamingClient
                .create(URL, subscription.getId(), "batch_limit=3&commit_timeout=1");

        client.start();
        waitFor(() -> Assert.assertFalse(client.isRunning()));
        Assert.assertTrue(isCommitTimeoutReached(client));

        client.start();
        waitFor(() -> Assert.assertFalse(client.isRunning()));
        Assert.assertTrue(isCommitTimeoutReached(client));

        client.start();
        waitFor(() -> Assert.assertFalse(client.isRunning()));
        Assert.assertTrue(isCommitTimeoutReached(client));

        client.start();
        waitFor(() -> Assert.assertFalse(client.isRunning()));
        // second batch is sent by Nakadi when a stream is closed
        Assert.assertEquals(2, client.getJsonBatches().size());
        Assert.assertEquals(1, client.getJsonBatches().get(0).getEvents().size());

        client.start();
        waitFor(() -> Assert.assertFalse(client.isRunning()));
        Assert.assertEquals(2, client.getJsonBatches().size());
        Assert.assertEquals(1, client.getJsonBatches().get(0).getEvents().size());

        client.start();
        waitFor(() -> Assert.assertFalse(client.isRunning()));
        Assert.assertEquals(2, client.getJsonBatches().size());
        Assert.assertEquals(1, client.getJsonBatches().get(0).getEvents().size());

        client.start();
        waitFor(() -> Assert.assertFalse(client.isRunning()));

        // check that we skipped over offset 0
        Assert.assertEquals("001-0001-000000000000000001", client.getJsonBatches().get(0).getCursor().getOffset());
    }

    @Test(timeout = 15000)
    public void whenCursorsAreResetTheDLQStateIsResetAsWell() throws IOException {
        final Subscription subscription = createAutoDLQSubscription(eventType, UnprocessableEventPolicy.SKIP_EVENT, 3);
        final TestStreamingClient client = TestStreamingClient
                .create(URL, subscription.getId(), "batch_limit=3&commit_timeout=1")
                .start();

        publishEvents(eventType.getName(), 50, i -> "{\"foo\":\"bar\"}");

        waitFor(() -> Assert.assertFalse(client.isRunning()));
        Assert.assertTrue(isCommitTimeoutReached(client));

        final List<SubscriptionCursor> cursorSnapshot = getSubscriptionCursors(subscription.getId());

        client.start();
        waitFor(() -> Assert.assertFalse(client.isRunning()));
        Assert.assertTrue(isCommitTimeoutReached(client));

        client.start();
        waitFor(() -> Assert.assertFalse(client.isRunning()));
        Assert.assertTrue(isCommitTimeoutReached(client));

        client.start();
        waitFor(() -> Assert.assertFalse(client.isRunning()));
        Assert.assertEquals(2, client.getJsonBatches().size());
        Assert.assertEquals(1, client.getJsonBatches().get(0).getEvents().size());

        resetCursorsWithToken(subscription.getId(), cursorSnapshot);

        client.start();
        waitFor(() -> Assert.assertFalse(client.isRunning()));
        Assert.assertEquals(3, client.getJsonBatches().get(0).getEvents().size());
    }

    @Test(timeout = 30_000)
    public void whenIsLookingForDeadLetterAndCommitComesThenContinueLooking() throws IOException, InterruptedException {
        final Subscription subscription = createAutoDLQSubscription(eventType, UnprocessableEventPolicy.SKIP_EVENT, 3);
        final TestStreamingClient client = TestStreamingClient
                .create(URL, subscription.getId(), "batch_limit=10&commit_timeout=1")
                .start();

        publishEvents(eventType.getName(), 50, i -> "{\"foo\":\"bar\"}");

        // reach commit timeout 3 times that Nakadi goes into mode to send single event in a batch to find the bad event
        waitFor(() -> Assert.assertFalse(client.isRunning()));
        Assert.assertTrue(isCommitTimeoutReached(client));
        Assert.assertEquals(10, client.getJsonBatches().get(0).getEvents().size());
        Assert.assertEquals("001-0001-000000000000000009", client.getJsonBatches().get(0).getCursor().getOffset());

        client.start();
        waitFor(() -> Assert.assertFalse(client.isRunning()));
        Assert.assertTrue(isCommitTimeoutReached(client));
        Assert.assertEquals(10, client.getJsonBatches().get(0).getEvents().size());
        Assert.assertEquals("001-0001-000000000000000009", client.getJsonBatches().get(0).getCursor().getOffset());

        client.start();
        waitFor(() -> Assert.assertFalse(client.isRunning()));
        Assert.assertTrue(isCommitTimeoutReached(client));
        Assert.assertEquals(10, client.getJsonBatches().get(0).getEvents().size());
        Assert.assertEquals("001-0001-000000000000000009", client.getJsonBatches().get(0).getCursor().getOffset());

        // receive a single event in a batch and commit it so that Nakadi sends the next batch with a single event
        // (since consumer was able to process the single event meaning the event is not problematic)
        client.start();
        waitFor(() -> Assert.assertFalse(client.getJsonBatches().isEmpty()));
        Assert.assertEquals(1, client.getJsonBatches().size());
        Assert.assertEquals(1, client.getJsonBatches().get(0).getEvents().size());
        Assert.assertEquals("001-0001-000000000000000000", client.getJsonBatches().get(0).getCursor().getOffset());

        final SubscriptionCursor cursor = client.getJsonBatches().get(0).getCursor();
        commitCursors(subscription.getId(), ImmutableList.of(cursor), client.getSessionId());

        // 2 because client was not closed
        waitFor(() -> Assert.assertEquals(2, client.getJsonBatches().size()));
        Assert.assertEquals(1, client.getJsonBatches().get(1).getEvents().size());
        Assert.assertEquals("001-0001-000000000000000001", client.getJsonBatches().get(1).getCursor().getOffset());
        waitFor(() -> Assert.assertFalse(client.isRunning()));

        // fail the next single event batch 3 times so that Nakadi understands
        // consumer is not able to process the event, so it should be skipped
        client.start();
        waitFor(() -> Assert.assertFalse(client.isRunning()));
        Assert.assertTrue(isCommitTimeoutReached(client));
        Assert.assertEquals(1, client.getJsonBatches().get(0).getEvents().size());
        Assert.assertEquals("001-0001-000000000000000001", client.getJsonBatches().get(0).getCursor().getOffset());

        client.start();
        waitFor(() -> Assert.assertFalse(client.isRunning()));
        Assert.assertTrue(isCommitTimeoutReached(client));
        Assert.assertEquals(1, client.getJsonBatches().get(0).getEvents().size());
        Assert.assertEquals("001-0001-000000000000000001", client.getJsonBatches().get(0).getCursor().getOffset());

        // now a single event should be skipped, but we continue getting them one by one until the failing batch end
        final TestStreamingClient client2 = TestStreamingClient
                .create(URL, subscription.getId(), "batch_limit=10&commit_timeout=1&stream_limit=20");

        client2.startWithAutocommit(batches -> {
                    // 8×1 + 1×10 + 1×2 + 1×"stream closed"
                    Assert.assertEquals(11, batches.size());

                    final List<StreamBatch> singleEventBatches = batches.subList(0, 8);
                    for (int i = 0; i < singleEventBatches.size(); ++i)  {
                        final StreamBatch batch = singleEventBatches.get(i);
                        Assert.assertEquals(1, batch.getEvents().size());
                    }

                    final StreamBatch theLast = batches.get(8);
                    Assert.assertEquals(10, theLast.getEvents().size());
                });
        waitFor(() -> Assert.assertFalse(client2.isRunning()), 15_000);

        // continue with normal batch size after skipping over the failing batch
        client.start();
        waitFor(() -> Assert.assertFalse(client.isRunning()));
        Assert.assertTrue(isCommitTimeoutReached(client));
        Assert.assertEquals(10, client.getJsonBatches().get(0).getEvents().size());
    }

    @Test(timeout = 20_000)
    public void whenIsLookingForDeadLetterAndSendAllEventsOneByOneThenBackToNormalBatchSize()
            throws InterruptedException, IOException {
        final Subscription subscription = createAutoDLQSubscription(eventType, UnprocessableEventPolicy.SKIP_EVENT, 3);
        final TestStreamingClient client = TestStreamingClient
                .create(URL, subscription.getId(), "batch_limit=10&commit_timeout=1&stream_limit=20")
                .start();

        publishEvents(eventType.getName(), 50, i -> "{\"foo\":\"bar\"}");

        // reach commit timeout 3 times that Nakadi goes into mode to send single event in a batch to find the bad event
        waitFor(() -> Assert.assertFalse(client.isRunning()));
        Assert.assertTrue(isCommitTimeoutReached(client));
        Assert.assertEquals(10, client.getJsonBatches().get(0).getEvents().size());
        Assert.assertEquals("001-0001-000000000000000009", client.getJsonBatches().get(0).getCursor().getOffset());

        client.start();
        waitFor(() -> Assert.assertFalse(client.isRunning()));
        Assert.assertTrue(isCommitTimeoutReached(client));
        Assert.assertEquals(10, client.getJsonBatches().get(0).getEvents().size());
        Assert.assertEquals("001-0001-000000000000000009", client.getJsonBatches().get(0).getCursor().getOffset());

        client.start();
        waitFor(() -> Assert.assertFalse(client.isRunning()));
        Assert.assertTrue(isCommitTimeoutReached(client));
        Assert.assertEquals(10, client.getJsonBatches().get(0).getEvents().size());
        Assert.assertEquals("001-0001-000000000000000009", client.getJsonBatches().get(0).getCursor().getOffset());

        // receive a single event in a batch and commit it so that Nakadi sends the next batch with a single event
        client.startWithAutocommit(batches -> {
            // 12 because the last one is stream limit reached debug info
            Assert.assertEquals(12, batches.size());

            batches.subList(0, 10).forEach(batch -> Assert.assertEquals(1, batch.getEvents().size()));

            final StreamBatch theLast = batches.get(10);
            Assert.assertEquals(10, theLast.getEvents().size());
        });

        // wait for commit thread
        waitFor(() -> Assert.assertFalse(client.isRunning()), 15_000);
    }

    @Test(timeout = 20_000)
    public void shouldSkipPoisonPillAndDeadLetterFoundInTheQueueLater() throws IOException, InterruptedException {

        final EventType eventType = NakadiTestUtils.createBusinessEventTypeWithPartitions(4);

        publishBusinessEventWithUserDefinedPartition(eventType.getName(),
                50, i -> String.format("bar%d", i), i -> String.valueOf(i % 4));

        final String poisonPillValue = "bar10";

        final Subscription subscription =
                createAutoDLQSubscription(eventType, UnprocessableEventPolicy.DEAD_LETTER_QUEUE, 3);

        // start looking for events in the DLQ store event type already (reading from END)
        final Subscription dlqStoreEventTypeSub = createSubscriptionForEventType("nakadi.dead.letter.queue");
        final TestStreamingClient dlqStoreClient = TestStreamingClient.create(URL,
                dlqStoreEventTypeSub.getId(), "batch_limit=1&stream_timeout=15");
        dlqStoreClient.startWithAutocommit(batches ->
                Assert.assertTrue("failed event should be found in the dead letter queue",
                        batches.stream()
                        .flatMap(b -> b.getEvents().stream())
                        .anyMatch(e ->
                                subscription.getId().equals(e.get("subscription_id")) &&
                                poisonPillValue.equals(e.getJSONObject("event").getString("foo")))));

        final AtomicReference<SubscriptionCursor> cursorWithPoisonPill = new AtomicReference<>();
        while (true) {
            final TestStreamingClient client = TestStreamingClient.create(
                    URL, subscription.getId(), "batch_limit=3&commit_timeout=1&stream_timeout=2");
            client.start(streamBatch -> {
                if (streamBatch.getEvents().stream()
                        .anyMatch(event -> poisonPillValue.equals(event.getString("foo")))) {
                    cursorWithPoisonPill.set(streamBatch.getCursor());
                    throw new RuntimeException("poison pill found");
                } else {
                    try {
                        NakadiTestUtils.commitCursors(
                                subscription.getId(), ImmutableList.of(streamBatch.getCursor()), client.getSessionId());
                    } catch (JsonProcessingException e) {
                        throw new RuntimeException(e);
                    }
                }
            });

            waitFor(() -> Assert.assertFalse(client.isRunning()));

            if (client.getJsonBatches().stream()
                    .filter(streamBatch -> cursorWithPoisonPill.get().getPartition()
                            .equals(streamBatch.getCursor().getPartition()))
                    .anyMatch(streamBatch -> streamBatch.getCursor().getOffset()
                            .compareTo(cursorWithPoisonPill.get().getOffset()) > 0)) {
                break;
            }
        }

        waitFor(() -> Assert.assertFalse(dlqStoreClient.isRunning()));
    }

    @Test(timeout = 35_000)
    public void testDlqModeOnlySendsSingleEventAndNotMore() throws IOException {
        publishEvents(eventType.getName(), 50, i -> "{\"foo\":\"bar" + i + "\"}");
        final int maxEventSendCount = 2;
        final Subscription subscription = createAutoDLQSubscription(eventType, UnprocessableEventPolicy.SKIP_EVENT,
                maxEventSendCount);
        final TestStreamingClient client = TestStreamingClient
                .create(URL, subscription.getId(), "batch_limit=3&commit_timeout=5&batch_flush_timeout=2");

        client.start();
        waitFor(() -> Assert.assertFalse(client.isRunning()));
        Assert.assertTrue(isCommitTimeoutReached(client));

        client.start();
        waitFor(() -> Assert.assertFalse(client.isRunning()));
        Assert.assertTrue(isCommitTimeoutReached(client));

        //DLQ MODE
        client.start();
        waitFor(() -> Assert.assertFalse(client.isRunning()));
        Assert.assertEquals(4, client.getJsonBatches().size()); // event batch, keepalive *2, commit timeout
        Assert.assertEquals(1, client.getJsonBatches().get(0).getEvents().size());
        Assert.assertThat(client.getJsonBatches().get(1).getEvents(), empty());
        Assert.assertThat(client.getJsonBatches().get(2).getEvents(), empty());
        Assert.assertTrue(isCommitTimeoutReached(client));

        client.start();
        waitFor(() -> Assert.assertFalse(client.isRunning()));
        Assert.assertEquals(4, client.getJsonBatches().size()); // event batch, keepalive *2, commit timeout
        Assert.assertEquals(1, client.getJsonBatches().get(0).getEvents().size());
        Assert.assertThat(client.getJsonBatches().get(1).getEvents(), empty());
        Assert.assertThat(client.getJsonBatches().get(2).getEvents(), empty());
        Assert.assertTrue(isCommitTimeoutReached(client));

        client.start();
        waitFor(() -> Assert.assertFalse(client.isRunning()));
        // check that we skipped over offset 0
        Assert.assertEquals("001-0001-000000000000000001", client.getJsonBatches().get(0).getCursor().getOffset());
    }

    private static boolean isCommitTimeoutReached(final TestStreamingClient client) {
        return client.getJsonBatches().stream()
                .filter(batch -> batch.getMetadata() != null)
                .anyMatch(batch -> batch.getMetadata().getDebug().equals("Commit timeout reached"));
    }

    private Subscription createAutoDLQSubscription(final EventType eventType,
                                                   final UnprocessableEventPolicy unprocessableEventPolicy,
                                                   final int maxEventSendCount) throws IOException {

        final SubscriptionBase subscription = RandomSubscriptionBuilder.builder()
                .withEventType(eventType.getName())
                .withStartFrom(BEGIN)
                .buildSubscriptionBase();
        subscription.setAnnotations(Map.of(
                        SUBSCRIPTION_MAX_EVENT_SEND_COUNT, Integer.toString(maxEventSendCount),
                        SUBSCRIPTION_UNPROCESSABLE_EVENT_POLICY, unprocessableEventPolicy.toString()));
        return createSubscription(subscription);
    }

    List<SubscriptionCursor> getSubscriptionCursors(final String sid) throws IOException {
        final Response response = given()
                .get("/subscriptions/{id}/cursors", sid);
        Assert.assertEquals(HttpStatus.SC_OK, response.getStatusCode());
        return MAPPER.readValue(
                    response.asInputStream(),
                    new TypeReference<ItemsWrapper<SubscriptionCursor>>() {})
                .getItems();
    }

    void resetCursorsWithToken(
            final String sid,
            final List<SubscriptionCursor> cursors) throws JsonProcessingException {
        final List<SubscriptionCursorWithoutToken> cursorsWithoutToken = cursors
                .stream()
                .map(c -> new SubscriptionCursorWithoutToken(c.getEventType(), c.getPartition(), c.getOffset()))
                .collect(Collectors.toList());
        resetCursors(sid, cursorsWithoutToken);
    }

    void resetCursors(
            final String sid,
            final List<SubscriptionCursorWithoutToken> cursors) throws JsonProcessingException {
        final Response response = given()
                .body(MAPPER.writeValueAsString(new ItemsWrapper<>(cursors)))
                .contentType(JSON)
                .patch("/subscriptions/{id}/cursors", sid);
        Assert.assertEquals(HttpStatus.SC_NO_CONTENT, response.getStatusCode());
    }

}
