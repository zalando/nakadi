package de.zalando.aruha.nakadi.webservice.hila;

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
import java.util.Optional;
import java.util.Set;

import static com.google.common.collect.Sets.intersection;
import static de.zalando.aruha.nakadi.utils.TestUtils.waitFor;
import static de.zalando.aruha.nakadi.webservice.utils.NakadiTestUtils.commitCursors;
import static de.zalando.aruha.nakadi.webservice.utils.NakadiTestUtils.createBusinessEventTypeWithPartitions;
import static de.zalando.aruha.nakadi.webservice.utils.NakadiTestUtils.createSubscription;
import static de.zalando.aruha.nakadi.webservice.utils.NakadiTestUtils.publishBusinessEventWithUserDefinedPartition;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;
import static java.util.stream.IntStream.range;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;

public class HilaRebalanceAT extends BaseAT {

    private EventType eventType;
    private Subscription subscription;

    @Before
    public void before() throws IOException {
        eventType = createBusinessEventTypeWithPartitions(8);
        subscription = createSubscription(ImmutableSet.of(eventType.getName()));
    }

    @Test(timeout = 30000)
    public void whenRebalanceThenPartitionsAreEquallyDistributedAndCommittedOffsetsAreConsidered() throws Exception {
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
                .create(URL, subscription.getId(), "stream_limit=20")
                .start();

        // wait for rebalance process to start
        Thread.sleep(1000);

        // write 5 more events to each partition
        range(0, 40)
                .forEach(x -> publishBusinessEventWithUserDefinedPartition(
                        eventType.getName(), "blah_" + x, String.valueOf(x % 8)));

        // wait till all event arrive
        waitFor(() -> assertThat(clientB.getBatches(), hasSize(20)));
        waitFor(() -> assertThat(clientA.getBatches(), hasSize(60)));

        // check that only half of partitions were streamed to client A after rebalance
        final Set<String> clientAPartitionsAfterRebalance = getUniquePartitionsStreamedToClient(clientA, 40, 60);
        assertThat(clientAPartitionsAfterRebalance, hasSize(4));

        // check that only half of partitions were streamed to client B
        final Set<String> clientBPartitions = getUniquePartitionsStreamedToClient(clientB);
        assertThat(clientBPartitions, hasSize(4));

        // check that different partitions were streamed to different clients
        assertThat(intersection(clientAPartitionsAfterRebalance, clientBPartitions), hasSize(0));

        // commit what we consumed, as clientB has already consumed what was required by stream_limit - it should
        // be closed right after everything is committed
        final List<Cursor> lastCursors = getLastCursorsForPartitions(clientA, clientAPartitionsAfterRebalance);
        lastCursors.addAll(getLastCursorsForPartitions(clientB, clientBPartitions));
        commitCursors(subscription.getId(), lastCursors);
        waitFor(() -> assertThat(clientB.isRunning(), is(false)));

        // wait for rebalance process to start
        Thread.sleep(1000);

        // write 5 more events to each partition
        range(0, 40)
                .forEach(x -> publishBusinessEventWithUserDefinedPartition(
                        eventType.getName(), "blah__" + x, String.valueOf(x % 8)));

        // check that after second rebalance all events were consumed by first client
        waitFor(() -> assertThat(clientA.getBatches(), hasSize(100)));
    }

    @Test(timeout = 15000)
    public void whenNotCommittedThenEventsAreReplayedAfterRebalance() {
        range(0, 20)
                .forEach(x -> publishBusinessEventWithUserDefinedPartition(
                        eventType.getName(), "blah" + x, String.valueOf(x % 8)));

        final TestStreamingClient clientA = TestStreamingClient
                .create(URL, subscription.getId(), "commit_timeout=2")
                .start();
        waitFor(() -> assertThat(clientA.getBatches(), hasSize(20)));

        final TestStreamingClient clientB = TestStreamingClient
                .create(URL, subscription.getId(), "")
                .start();

        // after commit_timeout of first client exceeds it is closed and all events are resent to second client
        waitFor(() -> assertThat(clientB.getBatches(), hasSize(20)));
    }

    public List<Cursor> getLastCursorsForPartitions(final TestStreamingClient client, final Set<String> partitions) {
        if (!client.getBatches().isEmpty()) {
            return partitions.stream()
                    .map(partition ->
                            client.getBatches()
                            .stream()
                            .filter(batch -> batch.getCursor().getPartition().equals(partition))
                            .reduce((a1, a2) -> a2)
                            .map(StreamBatch::getCursor))
                    .filter(Optional::isPresent)
                    .map(Optional::get)
                    .collect(toList());
        } else {
            throw new IllegalStateException();
        }
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
