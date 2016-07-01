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

        // check that only half of partitions were streamed to client A after rebalance
        final Set<String> clientAPartitionsAfterRebalance = getUniquePartitionsStreamedToClient(clientA, 40, 60);
        assertThat(clientAPartitionsAfterRebalance, hasSize(4));

        // check that only half of partitions were streamed to client B
        final Set<String> clientBPartitions = getUniquePartitionsStreamedToClient(clientB);
        assertThat(clientBPartitions, hasSize(4));

        // check that different partitions were streamed to different clients
        assertThat(intersection(clientAPartitionsAfterRebalance, clientBPartitions), hasSize(0));
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
