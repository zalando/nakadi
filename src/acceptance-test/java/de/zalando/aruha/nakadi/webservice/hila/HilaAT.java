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
import java.util.List;
import java.util.Set;

import static com.google.common.collect.Sets.intersection;
import static de.zalando.aruha.nakadi.utils.TestUtils.waitFor;
import static de.zalando.aruha.nakadi.webservice.hila.StreamBatch.singleEventBatch;
import static de.zalando.aruha.nakadi.webservice.utils.NakadiTestUtils.commitCursors;
import static de.zalando.aruha.nakadi.webservice.utils.NakadiTestUtils.createBusinessEventTypeWithPartitions;
import static de.zalando.aruha.nakadi.webservice.utils.NakadiTestUtils.createEventType;
import static de.zalando.aruha.nakadi.webservice.utils.NakadiTestUtils.createSubscription;
import static de.zalando.aruha.nakadi.webservice.utils.NakadiTestUtils.publishBusinessEventWithUserDefinedPartition;
import static de.zalando.aruha.nakadi.webservice.utils.NakadiTestUtils.publishEvent;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;
import static java.util.stream.IntStream.range;
import static java.util.stream.IntStream.rangeClosed;
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

    @Test(timeout = 30000)
    public void whenRebalanceThenPartitionsAreEquallyDistributedAndCommittedOffsetsAreConsidered() throws Exception {
        eventType = createBusinessEventTypeWithPartitions(8);
        subscription = createSubscription(ImmutableSet.of(eventType.getName()));

        // write 5 events to each partition
        range(0, 40)
                .forEach(x -> publishBusinessEventWithUserDefinedPartition(
                        eventType.getName(), "blah" + x, String.valueOf(x % 8)));

        // create a session
        final TestStreamingClient clientA = TestStreamingClient
                .create(URL, subscription.getId(), "")
                .start();
        waitFor(() -> assertThat(clientA.getBatches(), hasSize(40)));

        // check that we received 5 events for each partitions
        range(0, 8).forEach(partition ->
                assertThat(
                        clientA.getBatches().stream()
                                .filter(batch -> batch.getCursor().getPartition().equals(String.valueOf(partition)))
                                .count(),
                        equalTo(5L)));

        // commit what we consumed
        final List<Cursor> cursors = range(0, 8)
                .boxed()
                .map(partition -> new Cursor(String.valueOf(partition), "4"))
                .collect(toList());
        commitCursors(subscription.getId(), cursors);

        // create second session for the same subscription
        final TestStreamingClient clientB = TestStreamingClient
                .create(URL, subscription.getId(), "")
                .start();

        // wait for rebalance process to start
        Thread.sleep(1000);

        // write 5 more events to each partition
        range(0, 40)
                .forEach(x -> publishBusinessEventWithUserDefinedPartition(
                        eventType.getName(), "blahAgain" + x, String.valueOf(x % 8)));

        // wait till all event arrive
        waitFor(() -> assertThat(clientB.getBatches(), hasSize(20)));
        waitFor(() -> assertThat(clientA.getBatches(), hasSize(60)));

        // check that only half of partitions was streamed to client A after rebalance
        final Set<String> clientAPartitionsAfterRebalance = getUniquePartitionsStreamedToClient(clientA, 40, 60);
        assertThat(clientAPartitionsAfterRebalance, hasSize(4));

        // check that only half of partitions was streamed to client B
        final Set<String> clientBPartitions = getUniquePartitionsStreamedToClient(clientB);
        assertThat(clientBPartitions, hasSize(4));

        // check that different partitions were streamed to different clients
        assertThat(intersection(clientAPartitionsAfterRebalance, clientBPartitions), hasSize(0));
    }

    @Test(timeout = 15000)
    public void whenWindowSizeIsSetItIsRespected() throws Exception {

        range(0, 15).forEach(x -> publishEvent(eventType.getName(), "{\"blah\":\"foo\"}"));

        final TestStreamingClient client = TestStreamingClient
                .create(URL, subscription.getId(), "window_size=5")
                .start();

        waitFor(() -> assertThat(client.getBatches(), hasSize(5)));

        final Cursor lastBatchCursor = client.getBatches().get(client.getBatches().size() - 1).getCursor();
        commitCursors(subscription.getId(), ImmutableList.of(lastBatchCursor));

        waitFor(() -> assertThat(client.getBatches(), hasSize(10)));

        final Cursor cursor = client.getBatches().get(client.getBatches().size() - 4).getCursor();
        commitCursors(subscription.getId(), ImmutableList.of(cursor));

        waitFor(() -> assertThat(client.getBatches(), hasSize(12)));
    }

    @Test(timeout = 15000)
    public void whenCommitTimeoutReachedSessionIsClosed() throws Exception {

        publishEvent(eventType.getName(), "{\"blah\":\"foo\"}");

        final TestStreamingClient client = TestStreamingClient
                .create(URL, subscription.getId(), "commit_timeout=3")
                .start();

        waitFor(() -> assertThat(client.getBatches(), hasSize(1)));
        waitFor(() -> assertThat(client.isRunning(), is(false)), 5000);
    }

    @Test(timeout = 15000)
    public void whenStreamTimeoutReachedSessionIsClosed() throws Exception {

        publishEvent(eventType.getName(), "{\"blah\":\"foo\"}");

        final TestStreamingClient client = TestStreamingClient
                .create(URL, subscription.getId(), "stream_timeout=3")
                .start();

        waitFor(() -> assertThat(client.getBatches(), hasSize(1)));

        // to check that stream_timeout works we need to commit everything we consumed, in other case
        // Nakadi will wait for commit the amount of time defined by commit_timeout
        final Cursor lastBatchCursor = client.getBatches().get(client.getBatches().size() - 1).getCursor();
        commitCursors(subscription.getId(), ImmutableList.of(lastBatchCursor));

        waitFor(() -> assertThat(client.isRunning(), is(false)), 5000);
    }

    private Set<String> getUniquePartitionsStreamedToClient(final TestStreamingClient client) {
        return getUniquePartitionsStreamedToClient(client, 0, client.getBatches().size());
    }

    private Set<String> getUniquePartitionsStreamedToClient(final TestStreamingClient client, final int fromBatch,
                                                            final int toBatch) {
        return client.getBatches()
                .subList(fromBatch, toBatch)
                .stream()
                .map(batch -> batch.getCursor().getPartition())
                .distinct()
                .collect(toSet());
    }

}
