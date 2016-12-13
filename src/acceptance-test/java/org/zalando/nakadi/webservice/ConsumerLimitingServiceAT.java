package org.zalando.nakadi.webservice;

import com.google.common.collect.ImmutableList;
import org.apache.curator.framework.CuratorFramework;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.zalando.nakadi.exceptions.NoConnectionSlotsException;
import org.zalando.nakadi.repository.zookeeper.ZooKeeperHolder;
import org.zalando.nakadi.repository.zookeeper.ZooKeeperLockFactory;
import org.zalando.nakadi.service.ConnectionSlot;
import org.zalando.nakadi.service.ConsumerLimitingService;
import org.zalando.nakadi.utils.TestUtils;
import org.zalando.nakadi.webservice.utils.ZookeeperTestUtils;

import java.util.List;

import static java.text.MessageFormat.format;
import static java.util.stream.IntStream.range;
import static org.hamcrest.CoreMatchers.hasItem;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasSize;
import static org.mockito.Mockito.when;
import static org.zalando.nakadi.utils.TestUtils.randomUUID;

public class ConsumerLimitingServiceAT extends BaseAT {

    private static final CuratorFramework CURATOR = ZookeeperTestUtils.createCurator(ZOOKEEPER_URL);

    private ConsumerLimitingService limitingService;
    private String eventType;
    private String client;

    @Before
    public void before() {
        eventType = TestUtils.randomValidEventTypeName();
        client = TestUtils.randomTextString();

        final ZooKeeperHolder zkHolder = Mockito.mock(ZooKeeperHolder.class);
        when(zkHolder.get()).thenReturn(CURATOR);
        final ZooKeeperLockFactory zkLockFactory = new ZooKeeperLockFactory(zkHolder);
        limitingService = new ConsumerLimitingService(zkHolder, zkLockFactory, 5);
    }

    @Test
    public void whenAcquireConnectionSlotsThenDataInZk() throws Exception {
        final ImmutableList<String> partitions = ImmutableList.of("0", "1", "2", "3");

        final List<ConnectionSlot> connectionSlots = limitingService.acquireConnectionSlots(client, eventType,
                partitions);

        assertThat("4 connection slots were created in ZK", connectionSlots, hasSize(4));

        for (final String partition : partitions) {
            final String path = zkPathForConsumer(partition);

            assertThat("Node for partition should be created",
                    CURATOR.checkExists().forPath(path),
                    not(nullValue()));

            final List<String> children = CURATOR.getChildren().forPath(path);
            assertThat("Node for connection should be created",
                    children,
                    hasSize(1));

            final ConnectionSlot expectedSlot = new ConnectionSlot(client, eventType, partition, children.get(0));
            assertThat(connectionSlots, hasItem(expectedSlot));
        }
    }

    @Test(expected = NoConnectionSlotsException.class)
    public void whenNoFreeSlotsThenException() throws Exception {
        final String partition = "0";

        range(0, 5).forEach(x -> {
            try {
                final String path = zkPathForConsumer(partition) + "/" + randomUUID();
                CURATOR.create().creatingParentsIfNeeded().forPath(path);
            } catch (Exception e) {
                throw new AssertionError("Error occurred when accessing Zookeeper");
            }
        });

        limitingService.acquireConnectionSlots(client, eventType, ImmutableList.of(partition));
    }

    @Test
    public void whenReleaseSlotThatNodeDeletedInZk() throws Exception {
        final String connectionId = randomUUID();
        final String partition = "0";

        final String partitionPath = zkPathForConsumer(partition);
        final String connectionPath = partitionPath + "/" + connectionId;
        CURATOR.create().creatingParentsIfNeeded().forPath(connectionPath);

        final ImmutableList<ConnectionSlot> connectionSlots =
                ImmutableList.of(new ConnectionSlot(client, eventType, partition, connectionId));

        limitingService.releaseConnectionSlots(connectionSlots);

        assertThat("partition and connection Zk nodes should be deleted",
                CURATOR.checkExists().forPath(partitionPath),
                nullValue());
    }

    private String zkPathForConsumer(final String partition) {
        return format("/nakadi/consumers/connections/{0}|{1}|{2}", client, eventType, partition);
    }

}
