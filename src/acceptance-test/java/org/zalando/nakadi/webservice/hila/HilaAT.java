package org.zalando.nakadi.webservice.hila;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.zalando.nakadi.domain.Cursor;
import org.zalando.nakadi.domain.EventType;
import org.zalando.nakadi.domain.Subscription;
import org.zalando.nakadi.webservice.BaseAT;
import org.zalando.nakadi.webservice.utils.TestStreamingClient;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

import static com.jayway.restassured.RestAssured.given;
import static org.zalando.nakadi.utils.TestUtils.waitFor;
import static org.zalando.nakadi.webservice.hila.StreamBatch.singleEventBatch;
import static org.zalando.nakadi.webservice.utils.NakadiTestUtils.commitCursors;
import static org.zalando.nakadi.webservice.utils.NakadiTestUtils.createEventType;
import static org.zalando.nakadi.webservice.utils.NakadiTestUtils.createSubscription;
import static org.zalando.nakadi.webservice.utils.NakadiTestUtils.publishEvent;
import static java.text.MessageFormat.format;
import static java.util.stream.IntStream.range;
import static java.util.stream.IntStream.rangeClosed;
import static org.apache.http.HttpStatus.SC_CONFLICT;
import static org.apache.http.HttpStatus.SC_OK;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.not;

public class HilaAT extends BaseAT {

    private EventType eventType;
    private Subscription subscription;

    @Before
    public void before() throws IOException {
        // create event-type and subscribe to it
        eventType = createEventType();
        subscription = createSubscription(ImmutableSet.of(eventType.getName()));
    }

    @Test(timeout = 30000)
    public void whenOffsetIsCommittedNextSessionStartsFromNextEventAfterCommitted() throws Exception {
        // write 4 events to event-type
        rangeClosed(0, 3)
                .forEach(x -> publishEvent(eventType.getName(), "{\"blah\":\"foo" + x + "\"}"));

        // create session, read from subscription and wait for events to be sent
        final TestStreamingClient client = TestStreamingClient
                .create(URL, subscription.getId(), "stream_limit=2")
                .start();
        waitFor(() -> assertThat(client.getBatches(), hasSize(2)));

        // check that we have read first two events with correct offsets
        assertThat(client.getBatches().get(0), equalTo(singleEventBatch("0", "0", ImmutableMap.of("blah", "foo0"))));
        assertThat(client.getBatches().get(1), equalTo(singleEventBatch("0", "1", ImmutableMap.of("blah", "foo1"))));

        // commit offset that will also trigger session closing as we reached stream_limit and committed
        commitCursors(subscription.getId(), ImmutableList.of(client.getBatches().get(1).getCursor()));
        waitFor(() -> assertThat(client.isRunning(), is(false)));

        // create new session and read from subscription again
        client.start();
        waitFor(() -> assertThat(client.getBatches(), hasSize(2)));

        // check that we have read the next two events with correct offsets
        assertThat(client.getBatches().get(0), equalTo(singleEventBatch("0", "2", ImmutableMap.of("blah", "foo2"))));
        assertThat(client.getBatches().get(1), equalTo(singleEventBatch("0", "3", ImmutableMap.of("blah", "foo3"))));
    }

    @Test(timeout = 5000)
    public void whenCommitVeryFirstEventThenOk() throws Exception {
        publishEvent(eventType.getName(), "{\"blah\":\"foo\"}");

        // create session, read from subscription and wait for events to be sent
        final TestStreamingClient client = TestStreamingClient
                .create(URL, subscription.getId(), "")
                .start();
        waitFor(() -> assertThat(client.getBatches(), not(empty())));

        // commit and check that status is 200
        final int commitResult = commitCursors(subscription.getId(), ImmutableList.of(new Cursor("0", "0")));
        assertThat(commitResult, equalTo(SC_OK));
    }

    @Test(timeout = 15000)
    public void whenWindowSizeIsSetItIsConsidered() throws Exception {

        range(0, 15).forEach(x -> publishEvent(eventType.getName(), "{\"blah\":\"foo\"}"));

        final TestStreamingClient client = TestStreamingClient
                .create(URL, subscription.getId(), "window_size=5")
                .start();

        waitFor(() -> assertThat(client.getBatches(), hasSize(5)));

        Cursor cursorToCommit = client.getBatches().get(4).getCursor();
        commitCursors(subscription.getId(), ImmutableList.of(cursorToCommit));

        waitFor(() -> assertThat(client.getBatches(), hasSize(10)));

        cursorToCommit = client.getBatches().get(6).getCursor();
        commitCursors(subscription.getId(), ImmutableList.of(cursorToCommit));

        waitFor(() -> assertThat(client.getBatches(), hasSize(12)));
    }

    @Test(timeout = 10000)
    public void whenCommitTimeoutReachedSessionIsClosed() throws Exception {

        publishEvent(eventType.getName(), "{\"blah\":\"foo\"}");

        final TestStreamingClient client = TestStreamingClient
                .create(URL, subscription.getId(), "commit_timeout=1")
                .start();

        waitFor(() -> assertThat(client.getBatches(), hasSize(1)));
        waitFor(() -> assertThat(client.isRunning(), is(false)), 3000);
    }

    @Test(timeout = 15000)
    public void whenStreamTimeoutReachedSessionIsClosed() throws Exception {

        publishEvent(eventType.getName(), "{\"blah\":\"foo\"}");

        final TestStreamingClient client = TestStreamingClient
                .create(URL, subscription.getId(), "stream_timeout=3")
                .start();

        waitFor(() -> assertThat(client.getBatches(), hasSize(1)));

        // to check that stream_timeout works we need to commit everything we consumed, in other case
        // Nakadi will first wait till commit_timeout exceeds
        final Cursor lastBatchCursor = client.getBatches().get(client.getBatches().size() - 1).getCursor();
        commitCursors(subscription.getId(), ImmutableList.of(lastBatchCursor));

        waitFor(() -> assertThat(client.isRunning(), is(false)), 5000);
    }

    @Test(timeout = 10000)
    public void whenBatchLimitAndTimeoutAreSetTheyAreConsidered() throws Exception {

        range(0, 12).forEach(x -> publishEvent(eventType.getName(), "{\"blah\":\"foo\"}"));

        final TestStreamingClient client = TestStreamingClient
                .create(URL, subscription.getId(), "batch_limit=5&batch_flush_timeout=1")
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

}
