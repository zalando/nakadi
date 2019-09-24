package org.zalando.nakadi.webservice.hila;

import org.apache.curator.framework.CuratorFramework;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;
import org.zalando.nakadi.repository.zookeeper.ZooKeeperHolder;
import org.zalando.nakadi.service.subscription.model.Partition;
import org.zalando.nakadi.service.subscription.zk.NewZkSubscriptionClient;
import org.zalando.nakadi.service.subscription.zk.ZkSubscriptionClient;
import org.zalando.nakadi.utils.TestUtils;
import org.zalando.nakadi.webservice.BaseAT;
import org.zalando.nakadi.webservice.utils.ZookeeperTestUtils;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static org.mockito.Matchers.anyInt;
import static org.mockito.Mockito.when;

public class HilaRepartitionAT extends BaseAT {
    private static final CuratorFramework CURATOR = ZookeeperTestUtils.createCurator(ZOOKEEPER_URL);
    private ZooKeeperHolder zooKeeperHolder;
    private final String sid = TestUtils.randomUUID();
    private final String subscriptionId = TestUtils.randomUUID();
    private final String eventTypeName = "random";
    private final String secondEventTypeName = "random_et_2";

    @Test
    public void testSubscriptionRepartitioningWithSingleEventType() throws Exception {
        zooKeeperHolder = Mockito.mock(ZooKeeperHolder.class);
        Mockito.when(zooKeeperHolder.get()).thenReturn(CURATOR);
        when(zooKeeperHolder.getSubscriptionCurator(anyInt()))
                .thenReturn(new ZooKeeperHolder.DisposableCuratorFramework(CURATOR));

        final ZkSubscriptionClient subscriptionClient = new NewZkSubscriptionClient(
                subscriptionId,
                zooKeeperHolder,
                String.format("%s.%s", subscriptionId, sid),
                MAPPER,
                30000
        );

        final Partition[] eventTypePartitions = {new Partition(
                eventTypeName, "0", null, null, Partition.State.ASSIGNED)};
        setInitialTopology(eventTypePartitions);
        subscriptionClient.repartitionTopology(eventTypeName, 2);

        Assert.assertEquals(subscriptionClient.getTopology().getPartitions().length, 2);
        Assert.assertEquals(subscriptionClient.getTopology().getPartitions()[1].getPartition(), "1");
        Assert.assertEquals(subscriptionClient.getTopology().getPartitions()[1].getState(), Partition.State.UNASSIGNED);
        Assert.assertEquals(subscriptionClient.getTopology().getPartitions()[0].getState(), Partition.State.ASSIGNED);

        // test that offset path was created for new partition
        Assert.assertNotNull(CURATOR.checkExists().forPath(getOffsetPath(eventTypeName, "1")));
    }

    @Test
    public void testSubscriptionRepartitioningWithMultipleEventTypes() throws Exception {
        zooKeeperHolder = Mockito.mock(ZooKeeperHolder.class);
        Mockito.when(zooKeeperHolder.get()).thenReturn(CURATOR);
        when(zooKeeperHolder.getSubscriptionCurator(anyInt()))
                .thenReturn(new ZooKeeperHolder.DisposableCuratorFramework(CURATOR));

        final ZkSubscriptionClient subscriptionClient = new NewZkSubscriptionClient(
                subscriptionId,
                zooKeeperHolder,
                String.format("%s.%s", subscriptionId, sid),
                MAPPER,
                30000
        );

        final Partition[] eventTypePartitions = {
                new Partition(eventTypeName, "0", null, null, Partition.State.ASSIGNED),
                new Partition(secondEventTypeName, "0", null, null, Partition.State.ASSIGNED)};
        setInitialTopology(eventTypePartitions);
        subscriptionClient.repartitionTopology(eventTypeName, 2);

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
                new NewZkSubscriptionClient.Topology(partitions, null, 0));
        if (null == CURATOR.checkExists().forPath(topologyPath)) {
            CURATOR.create().creatingParentsIfNeeded().forPath(topologyPath, topologyData);
        } else {
            CURATOR.setData().forPath(topologyPath, topologyData);
        }
    }
}
