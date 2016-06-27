package de.zalando.aruha.nakadi.webservice.hila;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import de.zalando.aruha.nakadi.domain.Cursor;
import de.zalando.aruha.nakadi.domain.EventType;
import de.zalando.aruha.nakadi.domain.Subscription;
import de.zalando.aruha.nakadi.webservice.BaseAT;
import de.zalando.aruha.nakadi.webservice.utils.TestStreamingClient;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.stream.IntStream;

import static de.zalando.aruha.nakadi.utils.TestUtils.waitFor;
import static de.zalando.aruha.nakadi.webservice.hila.StreamBatch.singleEventBatch;
import static de.zalando.aruha.nakadi.webservice.utils.NakadiTestUtils.commitCursors;
import static de.zalando.aruha.nakadi.webservice.utils.NakadiTestUtils.createEventType;
import static de.zalando.aruha.nakadi.webservice.utils.NakadiTestUtils.createSubscription;
import static de.zalando.aruha.nakadi.webservice.utils.NakadiTestUtils.publishMessage;
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
        IntStream
                .rangeClosed(0, 3)
                .forEach(x -> publishMessage(eventType.getName(), "{\"blah\":\"foo" + x + "\"}"));

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
        publishMessage(eventType.getName(), "{\"blah\":\"foo\"}");

        // create session, read from subscription and wait for events to be sent
        final TestStreamingClient client = TestStreamingClient
                .create(URL, subscription.getId(), "")
                .start();
        waitFor(() -> assertThat(client.getBatches(), not(empty())));

        // commit and check that status is 200
        final int commitResult = commitCursors(subscription.getId(), ImmutableList.of(new Cursor("0", "0")));
        assertThat(commitResult, equalTo(SC_OK));
    }

}
