package org.zalando.nakadi.webservice.hila;

import org.apache.curator.framework.CuratorFramework;
import org.hamcrest.MatcherAssert;
import org.hamcrest.Matchers;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zalando.nakadi.domain.EventType;
import org.zalando.nakadi.domain.Subscription;
import org.zalando.nakadi.domain.SubscriptionBase;
import org.zalando.nakadi.repository.zookeeper.ZooKeeperHolder;
import org.zalando.nakadi.service.subscription.model.Partition;
import org.zalando.nakadi.service.subscription.zk.NewZkSubscriptionClient;
import org.zalando.nakadi.service.subscription.zk.ZkSubscriptionClient;
import org.zalando.nakadi.utils.RandomSubscriptionBuilder;
import org.zalando.nakadi.utils.TestUtils;
import org.zalando.nakadi.webservice.BaseAT;
import org.zalando.nakadi.webservice.utils.NakadiTestUtils;
import org.zalando.nakadi.webservice.utils.TestStreamingClient;
import org.zalando.nakadi.webservice.utils.ZookeeperTestUtils;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class HilaRepartitionAT extends BaseAT {
    private static final Logger LOG = LoggerFactory.getLogger(HilaRepartitionAT.class);
    private static final CuratorFramework CURATOR = ZookeeperTestUtils.createCurator(ZOOKEEPER_URL);
    private final String subscriptionId = TestUtils.randomUUID();
    private final String eventTypeName = "random";
    private final String secondEventTypeName = "random_et_2";

    @Test
    public void testSubscriptionRepartitioningWithSingleEventType() throws Exception {
        final ZkSubscriptionClient subscriptionClient = new NewZkSubscriptionClient(
                subscriptionId,
                new ZooKeeperHolder.DisposableCuratorFramework(CURATOR),
                MAPPER
        );

        final Partition[] eventTypePartitions = {new Partition(
                eventTypeName, "0", null, null, Partition.State.ASSIGNED)};
        setInitialTopology(eventTypePartitions);
        subscriptionClient.repartitionTopology(eventTypeName, 2, "001-0001--1");

        Assert.assertEquals(subscriptionClient.getTopology().getPartitions().length, 2);
        Assert.assertEquals(subscriptionClient.getTopology().getPartitions()[1].getPartition(), "1");
        Assert.assertEquals(subscriptionClient.getTopology().getPartitions()[1].getState(), Partition.State.UNASSIGNED);
        Assert.assertEquals(subscriptionClient.getTopology().getPartitions()[0].getState(), Partition.State.ASSIGNED);

        // test that offset path was created for new partition
        Assert.assertNotNull(CURATOR.checkExists().forPath(getOffsetPath(eventTypeName, "1")));
    }

    @Test
    public void testSubscriptionRepartitioningWithMultipleEventTypes() throws Exception {
        final ZkSubscriptionClient subscriptionClient = new NewZkSubscriptionClient(
                subscriptionId,
                new ZooKeeperHolder.DisposableCuratorFramework(CURATOR),
                MAPPER
        );

        final Partition[] eventTypePartitions = {
                new Partition(eventTypeName, "0", null, null, Partition.State.ASSIGNED),
                new Partition(secondEventTypeName, "0", null, null, Partition.State.ASSIGNED)};
        setInitialTopology(eventTypePartitions);
        subscriptionClient.repartitionTopology(eventTypeName, 2, "001-0001--1");

        Assert.assertEquals(subscriptionClient.getTopology().getPartitions().length, 3);
        final List<Partition> eTPartitions =
                Arrays.stream(subscriptionClient.getTopology().getPartitions())
                        .filter(p -> p.getEventType().equals(secondEventTypeName)).collect(Collectors.toList());
        Assert.assertEquals(eTPartitions.get(0).getState(), Partition.State.ASSIGNED);
    }

    private String subscriptionPath() {
        final String parentPath = "/nakadi/subscriptions";
        return String.format("%s/%s", parentPath, subscriptionId);
    }

    private String getOffsetPath(final String eventTypeName, final String partition) {
        return String.format("%s/offsets/%s/%s", subscriptionPath(), eventTypeName, partition);
    }

    private void setInitialTopology(final Partition[] partitions) throws Exception {
        final String topologyPath = subscriptionPath() + "/topology";
        final byte[] topologyData = MAPPER.writeValueAsBytes(
                new NewZkSubscriptionClient.Topology(partitions, 0));
        if (null == CURATOR.checkExists().forPath(topologyPath)) {
            CURATOR.create().creatingParentsIfNeeded().forPath(topologyPath, topologyData);
        } else {
            CURATOR.setData().forPath(topologyPath, topologyData);
        }
    }

    @Test(timeout = 30000)
    public void whenEventTypeRepartitionedSubscriptionStartsStreamNewPartitions() throws Exception {
        final EventType eventType = NakadiTestUtils.createBusinessEventTypeWithPartitions(1);
        final Subscription subscription = NakadiTestUtils.createSubscription(
                RandomSubscriptionBuilder.builder()
                        .withEventType(eventType.getName())
                        .withStartFrom(SubscriptionBase.InitialPosition.BEGIN)
                        .buildSubscriptionBase());

        NakadiTestUtils.publishBusinessEventWithUserDefinedPartition(
                eventType.getName(), 1, x -> "{\"foo\":\"bar\"}", p -> "0");

        // create session, read from subscription and wait for events to be sent
        final TestStreamingClient client = TestStreamingClient
                .create(URL, subscription.getId(), "")
                .startWithAutocommit(streamBatches -> LOG.info("{}", streamBatches));

        TestUtils.waitFor(() -> MatcherAssert.assertThat(client.getJsonBatches(), Matchers.hasSize(1)));
        Assert.assertEquals("0", client.getJsonBatches().get(0).getCursor().getPartition());

        NakadiTestUtils.repartitionEventType(eventType, 2);
        TestUtils.waitFor(() -> MatcherAssert.assertThat(client.isRunning(), Matchers.is(false)));

        Thread.sleep(1500);

        final TestStreamingClient clientAfterRepartitioning = TestStreamingClient
                .create(URL, subscription.getId(), "")
                .startWithAutocommit(streamBatches -> LOG.info("{}", streamBatches));

        NakadiTestUtils.publishBusinessEventWithUserDefinedPartition(
                eventType.getName(), 1, x -> "{\"foo\":\"bar" + x + "\"}", p -> "1");

        TestUtils.waitFor(() -> MatcherAssert.assertThat(
                clientAfterRepartitioning.getJsonBatches(), Matchers.hasSize(1)));
        Assert.assertEquals("1", clientAfterRepartitioning.getJsonBatches().get(0).getCursor().getPartition());
    }

    @Test(timeout = 30000)
    public void shouldRepartitionTimelinedEventType() throws Exception {
        final EventType eventType = NakadiTestUtils.createBusinessEventTypeWithPartitions(1);
        final Subscription subscription = NakadiTestUtils.createSubscription(
                RandomSubscriptionBuilder.builder()
                        .withEventType(eventType.getName())
                        .withStartFrom(SubscriptionBase.InitialPosition.BEGIN)
                        .buildSubscriptionBase());

        NakadiTestUtils.publishBusinessEventWithUserDefinedPartition(
                eventType.getName(), 1, x -> "{\"foo\":\"bar\"}", p -> "0");

        // create session, read from subscription and wait for events to be sent
        final TestStreamingClient client = TestStreamingClient
                .create(URL, subscription.getId(), "")
                .startWithAutocommit(streamBatches -> LOG.info("{}", streamBatches));

        TestUtils.waitFor(() -> MatcherAssert.assertThat(client.getJsonBatches(), Matchers.hasSize(1)));
        Assert.assertEquals("0", client.getJsonBatches().get(0).getCursor().getPartition());

        NakadiTestUtils.createTimeline(eventType.getName());

        NakadiTestUtils.repartitionEventType(eventType, 2);
        TestUtils.waitFor(() -> MatcherAssert.assertThat(client.isRunning(), Matchers.is(false)));

        Thread.sleep(1500);

        final TestStreamingClient clientAfterRepartitioning = TestStreamingClient
                .create(URL, subscription.getId(), "")
                .startWithAutocommit(streamBatches -> LOG.info("{}", streamBatches));

        NakadiTestUtils.publishBusinessEventWithUserDefinedPartition(
                eventType.getName(), 1, x -> "{\"foo\":\"bar" + x + "\"}", p -> "1");

        TestUtils.waitFor(() -> MatcherAssert.assertThat(
                clientAfterRepartitioning.getJsonBatches(), Matchers.hasSize(1)));
        Assert.assertEquals("1", clientAfterRepartitioning.getJsonBatches().get(0).getCursor().getPartition());
    }
}
