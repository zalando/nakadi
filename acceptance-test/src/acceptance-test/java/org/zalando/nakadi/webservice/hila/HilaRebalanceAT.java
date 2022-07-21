package org.zalando.nakadi.webservice.hila;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.hamcrest.Matchers;
import org.junit.Before;
import org.junit.Test;
import org.springframework.http.HttpStatus;
import org.zalando.nakadi.domain.EventType;
import org.zalando.nakadi.domain.Subscription;
import org.zalando.nakadi.domain.SubscriptionBase;
import org.zalando.nakadi.utils.RandomSubscriptionBuilder;
import org.zalando.nakadi.view.SubscriptionCursor;
import org.zalando.nakadi.webservice.BaseAT;
import org.zalando.nakadi.webservice.utils.TestStreamingClient;

import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static com.google.common.collect.Sets.intersection;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;
import static java.util.stream.IntStream.range;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasSize;
import static org.zalando.nakadi.domain.SubscriptionBase.InitialPosition.BEGIN;
import static org.zalando.nakadi.utils.TestUtils.waitFor;
import static org.zalando.nakadi.webservice.utils.NakadiTestUtils.commitCursors;
import static org.zalando.nakadi.webservice.utils.NakadiTestUtils.createBusinessEventTypeWithPartitions;
import static org.zalando.nakadi.webservice.utils.NakadiTestUtils.createSubscription;
import static org.zalando.nakadi.webservice.utils.NakadiTestUtils.getNumberOfAssignedStreams;
import static org.zalando.nakadi.webservice.utils.NakadiTestUtils.publishBusinessEventWithUserDefinedPartition;

public class HilaRebalanceAT extends BaseAT {

    private EventType eventType;
    private Subscription subscription;

    @Before
    public void before() throws IOException {
        eventType = createBusinessEventTypeWithPartitions(8);
        final SubscriptionBase subscriptionBase = RandomSubscriptionBuilder.builder()
                .withEventType(eventType.getName())
                .withStartFrom(BEGIN)
                .buildSubscriptionBase();
        subscription = createSubscription(subscriptionBase);
    }

    @Test(timeout = 30000)
    public void whenRebalanceThenPartitionsAreEquallyDistributedAndCommittedOffsetsAreConsidered() throws Exception {
        // write 5 events to each partition
        publishBusinessEventWithUserDefinedPartition(
                eventType.getName(),
                40,
                x -> "blah" + x,
                x -> String.valueOf(x % 8));

        // create a session
        final TestStreamingClient clientA = TestStreamingClient
                .create(URL, subscription.getId(), "max_uncommitted_events=100")
                .start();
        waitFor(() -> assertThat(clientA.getJsonBatches(), hasSize(40)));

        // check that we received 5 events for each partitions
        range(0, 8).forEach(partition ->
                assertThat(
                        clientA.getJsonBatches().stream()
                                .filter(batch -> batch.getCursor().getPartition().equals(String.valueOf(partition)))
                                .count(),
                        equalTo(5L)));

        // commit what we consumed
        final List<SubscriptionCursor> cursors = range(0, 8)
                .boxed()
                .map(partition -> new SubscriptionCursor(String.valueOf(partition), "4", eventType.getName(), "token"))
                .collect(toList());
        commitCursors(subscription.getId(), cursors, clientA.getSessionId());

        // create second session for the same subscription
        final TestStreamingClient clientB = TestStreamingClient
                .create(URL, subscription.getId(), "stream_limit=20&max_uncommitted_events=100")
                .start();

        // wait for rebalance process to finish
        waitFor(() -> assertThat(getNumberOfAssignedStreams(subscription.getId()), Matchers.is(2)));

        // write 5 more events to each partition
        publishBusinessEventWithUserDefinedPartition(
                eventType.getName(),
                40,
                x -> "blah_" + x,
                x -> String.valueOf(x % 8));

        // wait till all events arrive
        waitFor(() -> assertThat(clientB.getJsonBatches(), hasSize(20)));
        waitFor(() -> assertThat(clientA.getJsonBatches(), hasSize(60)));

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
        final List<SubscriptionCursor> lastCursorsA = getLastCursorsForPartitions(clientA,
                clientAPartitionsAfterRebalance);
        final List<SubscriptionCursor> lastCursorsB = getLastCursorsForPartitions(clientB, clientBPartitions);
        commitCursors(subscription.getId(), lastCursorsA, clientA.getSessionId());
        commitCursors(subscription.getId(), lastCursorsB, clientB.getSessionId());
        waitFor(() -> assertThat(clientB.isRunning(), Matchers.is(false)));

        // wait for rebalance process to finish
        waitFor(() -> assertThat(getNumberOfAssignedStreams(subscription.getId()), Matchers.is(1)));

        // write 5 more events to each partition
        publishBusinessEventWithUserDefinedPartition(
                eventType.getName(), 40, x -> "blah__" + x, x -> String.valueOf(x % 8));

        // check that after second rebalance all events were consumed by first client
        waitFor(() -> assertThat(clientA.getJsonBatches(), hasSize(100)));
    }

    @Test(timeout = 15000)
    public void whenDirectlyRequestPartitionsTheyAssignedCorrectly() throws IOException {
        // publish 2 events to each partition
        publishBusinessEventWithUserDefinedPartition(
                eventType.getName(), 16, x -> "blah" + x, x -> String.valueOf(x % 8));

        // start a stream requesting to read from partitions 5, 6
        final TestStreamingClient client = new TestStreamingClient(URL, subscription.getId(), "", Optional.empty(),
                Optional.of("{\"partitions\":[" +
                        "{\"event_type\":\"" + eventType.getName() + "\",\"partition\":\"6\"}," +
                        "{\"event_type\":\"" + eventType.getName() + "\",\"partition\":\"5\"}" +
                        "]}"));
        client.start();

        // wait till we receive 4 batches (2 per partition)
        waitFor(() -> assertThat(client.getJsonBatches(), hasSize(4)));

        // check that all batches are from partitions 5, 6
        checkAllEventsAreFromPartitions(client, ImmutableSet.of("5", "6"));
    }

    @Test(timeout = 15000)
    public void whenMixedStreamsThenPartitionsAssignedCorrectly() throws IOException, InterruptedException {

        // start 2 streams not specifying partitions directly
        final TestStreamingClient autoClient1 = new TestStreamingClient(URL, subscription.getId(), "");
        autoClient1.start();
        final TestStreamingClient autoClient2 = new TestStreamingClient(URL, subscription.getId(), "");
        autoClient2.start();

        // start a stream requesting to read from partition 6
        final TestStreamingClient directClient1 = new TestStreamingClient(URL, subscription.getId(), "",
                Optional.empty(),
                Optional.of("{\"partitions\":[" +
                        "{\"event_type\":\"" + eventType.getName() + "\",\"partition\":\"6\"}]}"));
        directClient1.start();

        // start a stream requesting to read from partition 7
        final TestStreamingClient directClient2 = new TestStreamingClient(URL, subscription.getId(), "",
                Optional.empty(),
                Optional.of("{\"partitions\":[" +
                        "{\"event_type\":\"" + eventType.getName() + "\",\"partition\":\"7\"}]}"));
        directClient2.start();

        // wait for rebalance to finish
        waitFor(() -> assertThat(getNumberOfAssignedStreams(subscription.getId()), Matchers.is(4)));

        // publish 2 events to each partition
        publishBusinessEventWithUserDefinedPartition(
                eventType.getName(), 16, x -> "blah" + x, x -> String.valueOf(x % 8));

        // we should receive 2 batches for streams that directly requested 1 partition
        waitFor(() -> assertThat(directClient1.getJsonBatches(), hasSize(2)));
        checkAllEventsAreFromPartitions(directClient1, ImmutableSet.of("6"));

        waitFor(() -> assertThat(directClient2.getJsonBatches(), hasSize(2)));
        checkAllEventsAreFromPartitions(directClient2, ImmutableSet.of("7"));

        // we should receive 6 batches for streams that use auto balance (they read 3 partitions each)
        waitFor(() -> assertThat(autoClient1.getJsonBatches(), hasSize(6)));
        waitFor(() -> assertThat(autoClient2.getJsonBatches(), hasSize(6)));
    }

    @Test(timeout = 15000)
    public void checkDirectAssignmentCorrectlyCapturesAndReleasesPartition() throws IOException, InterruptedException {
        // launch two clients: one using auto-rebalance, second one directly reading from partition 6
        final TestStreamingClient autoClient = new TestStreamingClient(URL, subscription.getId(),
                "max_uncommitted_events=100");
        autoClient.start();
        final TestStreamingClient directClient = new TestStreamingClient(URL, subscription.getId(), "",
                Optional.empty(),
                Optional.of("{\"stream_limit\":1,\"partitions\":[" +
                        "{\"event_type\":\"" + eventType.getName() + "\",\"partition\":\"6\"}]}"));
        directClient.start();

        // wait for rebalance to finish and send 1 event to each partition
        waitFor(() -> assertThat(getNumberOfAssignedStreams(subscription.getId()), Matchers.is(2)));
        publishBusinessEventWithUserDefinedPartition(
                eventType.getName(), 8, x -> "blah" + x, x -> String.valueOf(x % 8));

        waitFor(() -> assertThat(autoClient.getJsonBatches(), hasSize(7)));
        checkAllEventsAreFromPartitions(autoClient, ImmutableSet.of("0", "1", "2", "3", "4", "5", "7"));
        waitFor(() -> assertThat(directClient.getJsonBatches(), hasSize(1)));
        checkAllEventsAreFromPartitions(directClient, ImmutableSet.of("6"));

        // commit cursors and wait for stream to be closed (because of reaching stream_limit)
        commitCursors(
                subscription.getId(),
                directClient.getJsonBatches().stream().map(StreamBatch::getCursor).collect(Collectors.toList()),
                directClient.getSessionId());
        waitFor(() -> assertThat(directClient.isRunning(), Matchers.is(false)));


        // send 1 event to each partition
        publishBusinessEventWithUserDefinedPartition(
                eventType.getName(), 8, x -> "blah" + x, x -> String.valueOf(x % 8));

        // the client with auto-balancing should now get all 8 new events
        waitFor(() -> assertThat(autoClient.getJsonBatches(), hasSize(7 + 8)));
        checkAllEventsAreFromPartitions(autoClient, ImmutableSet.of("0", "1", "2", "3", "4", "5", "6", "7"));
    }

    @Test(timeout = 15000)
    public void whenTwoStreamsDirectlyRequestOnePartitionThenConflict() throws IOException, InterruptedException {

        // first stream wants to read partition 6
        final TestStreamingClient client1 = new TestStreamingClient(URL, subscription.getId(), "", Optional.empty(),
                Optional.of("{\"partitions\":[" +
                        "{\"event_type\":\"" + eventType.getName() + "\",\"partition\":\"6\"}" +
                        "]}"));
        client1.start();
        waitFor(() -> assertThat(client1.getResponseCode(), Matchers.is(HttpStatus.OK.value())));

        // second stream wants to read partitions 5 and 6
        final TestStreamingClient client2 = new TestStreamingClient(URL, subscription.getId(), "", Optional.empty(),
                Optional.of("{\"partitions\":[" +
                        "{\"event_type\":\"" + eventType.getName() + "\",\"partition\":\"5\"}," +
                        "{\"event_type\":\"" + eventType.getName() + "\",\"partition\":\"6\"}" +
                        "]}"));
        client2.start();

        // check that second client was disconnected and received status code 409
        waitFor(() -> assertThat(client2.isRunning(), Matchers.is(false)));
        assertThat(client2.getResponseCode(), Matchers.is(HttpStatus.CONFLICT.value()));
    }

    @Test(timeout = 15000)
    public void whenNotCommittedThenEventsAreReplayedAfterRebalance() {
        publishBusinessEventWithUserDefinedPartition(
                eventType.getName(), 2, x -> "blah" + x, x -> String.valueOf(x % 8));

        final TestStreamingClient clientA = TestStreamingClient
                .create(URL, subscription.getId(), "")
                .start();
        waitFor(() -> assertThat(clientA.getJsonBatches(), hasSize(2)));

        final TestStreamingClient clientB = TestStreamingClient
                .create(URL, subscription.getId(), "")
                .start();

        // after commit_timeout of first client exceeds it is closed and all events are resent to second client
        waitFor(() -> assertThat(clientB.getJsonBatches(), hasSize(2)), 10000);
    }

    @Test(timeout = 15000)
    public void whenNoFreeSlotsForAutoClientThenConflict() throws IOException, InterruptedException {
        // direct client reads all but one partition
        final TestStreamingClient directClient = new TestStreamingClient(URL, subscription.getId(), "",
                Optional.empty(),
                Optional.of("{\"partitions\":[" +
                        "{\"event_type\":\"" + eventType.getName() + "\",\"partition\":\"0\"}," +
                        "{\"event_type\":\"" + eventType.getName() + "\",\"partition\":\"1\"}," +
                        "{\"event_type\":\"" + eventType.getName() + "\",\"partition\":\"2\"}," +
                        "{\"event_type\":\"" + eventType.getName() + "\",\"partition\":\"3\"}," +
                        "{\"event_type\":\"" + eventType.getName() + "\",\"partition\":\"4\"}," +
                        "{\"event_type\":\"" + eventType.getName() + "\",\"partition\":\"5\"}," +
                        "{\"event_type\":\"" + eventType.getName() + "\",\"partition\":\"6\"}" +
                        "]}"));
        directClient.start();
        waitFor(() -> assertThat(directClient.getResponseCode(), Matchers.is(HttpStatus.OK.value())));

        // first auto-balance client reads remaining partition
        final TestStreamingClient autoClient1 = new TestStreamingClient(URL, subscription.getId(), "");
        autoClient1.start();
        waitFor(() -> assertThat(autoClient1.getResponseCode(), Matchers.is(HttpStatus.OK.value())));

        // second auto-balance client has nothing to read - should get 409 (Conflict) status code
        final TestStreamingClient autoClient2 = new TestStreamingClient(URL, subscription.getId(), "");
        autoClient2.start();
        waitFor(() -> assertThat(autoClient2.getResponseCode(), Matchers.is(HttpStatus.CONFLICT.value())));
    }

    @Test(timeout = 15000)
    public void testCommitWhenDirectAssignment() throws Exception {
        // connect with 1 stream directly requesting one partition
        final TestStreamingClient client = new TestStreamingClient(URL, subscription.getId(), "batch_flush_timeout=1",
                Optional.empty(),
                Optional.of("{\"partitions\":[" +
                        "{\"event_type\":\"" + eventType.getName() + "\",\"partition\":\"0\"}]}"));
        client.start();
        // wait for rebalance to finish
        waitFor(() -> assertThat(getNumberOfAssignedStreams(subscription.getId()), Matchers.is(1)));
        // publish 1 event
        publishBusinessEventWithUserDefinedPartition(eventType.getName(), 1, x -> "blah", x -> "0");
        // wait for event to come
        waitFor(() -> assertThat(client.getJsonBatches(), hasSize(1)));

        // commit cursor
        final int commitStatusCode = commitCursors(subscription.getId(),
                ImmutableList.of(client.getJsonBatches().get(0).getCursor()), client.getSessionId());

        // check that we get 204
        assertThat(commitStatusCode, Matchers.is(HttpStatus.NO_CONTENT.value()));
    }

    @Test(timeout = 15000)
    public void testAtLeastOneClientGets409OnTheSamePartitionRequest() throws Exception {
        final TestStreamingClient client1 = new TestStreamingClient(
                URL, subscription.getId(), "batch_flush_timeout=1",
                Optional.empty(),
                Optional.of("{\"partitions\":[" +
                        "{\"event_type\":\"" + eventType.getName() + "\",\"partition\":\"0\"}]}"));
        final TestStreamingClient client2 = new TestStreamingClient(
                URL, subscription.getId(), "batch_flush_timeout=1",
                Optional.empty(),
                Optional.of("{\"partitions\":[" +
                        "{\"event_type\":\"" + eventType.getName() + "\",\"partition\":\"0\"}]}"));
        client1.start();
        client2.start();

        waitFor(() -> assertThat("at least one client should get 409 conflict",
                client1.getResponseCode() == HttpStatus.CONFLICT.value() ||
                        client2.getResponseCode() == HttpStatus.CONFLICT.value()));
    }

    public List<SubscriptionCursor> getLastCursorsForPartitions(final TestStreamingClient client,
                                                                final Set<String> partitions) {
        if (!client.getJsonBatches().isEmpty()) {
            return partitions.stream()
                    .map(partition -> client.getJsonBatches().stream()
                            .filter(batch -> batch.getCursor().getPartition().equals(partition))
                            .reduce((batch1, batch2) -> batch2)
                            .map(StreamBatch::getCursor))
                    .filter(Optional::isPresent)
                    .map(Optional::get)
                    .collect(toList());
        } else {
            throw new IllegalStateException();
        }
    }

    private void checkAllEventsAreFromPartitions(final TestStreamingClient clientA, final Set<String> partitions) {
        // check that all batches belong to the specified set of partitions
        final List<StreamBatch> batches = clientA.getJsonBatches();
        final long batchesFromCorrectPartitions = batches.stream()
                .filter(b -> partitions.contains(b.getCursor().getPartition()))
                .count();
        assertThat(batchesFromCorrectPartitions, Matchers.is((long) batches.size()));
        // check that all partitions are present in batches
        final long partitionsWithNoBatch = partitions.stream()
                .filter(p -> !batches.stream().anyMatch(b -> b.getCursor().getPartition().equals(p)))
                .count();
        assertThat(partitionsWithNoBatch, Matchers.is(0L));
    }

    private Set<String> getUniquePartitionsStreamedToClient(final TestStreamingClient client) {
        return getUniquePartitionsStreamedToClient(client, 0, client.getJsonBatches().size());
    }

    private Set<String> getUniquePartitionsStreamedToClient(final TestStreamingClient client, final int fromBatch,
                                                            final int toBatch) {
        return client.getJsonBatches()
                .subList(fromBatch, toBatch)
                .stream()
                .map(batch -> batch.getCursor().getPartition())
                .distinct()
                .collect(toSet());
    }

}
