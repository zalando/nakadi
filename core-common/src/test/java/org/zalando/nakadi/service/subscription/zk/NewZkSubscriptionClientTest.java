package org.zalando.nakadi.service.subscription.zk;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.api.BackgroundPathAndBytesable;
import org.apache.curator.framework.api.DeleteBuilder;
import org.apache.curator.framework.api.GetDataBuilder;
import org.apache.curator.framework.api.SetDataBuilder;
import org.apache.curator.framework.api.WatchPathable;
import org.apache.zookeeper.KeeperException;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.zalando.nakadi.domain.EventTypePartition;
import org.zalando.nakadi.repository.zookeeper.CuratorFrameworkRotator;
import org.zalando.nakadi.repository.zookeeper.ZooKeeperHolder;
import org.zalando.nakadi.service.subscription.model.Partition;
import org.zalando.nakadi.service.subscription.model.Session;

import java.util.Collections;

public class NewZkSubscriptionClientTest {

    private final CuratorFramework curator = Mockito.mock(CuratorFramework.class);
    private final CuratorFrameworkRotator curatorFrameworkRotator = Mockito.mock(CuratorFrameworkRotator.class);
    private final GetDataBuilder getDataBuilder = Mockito.mock(GetDataBuilder.class);
    private final SetDataBuilder setDataBuilder = Mockito.mock(SetDataBuilder.class);
    private final BackgroundPathAndBytesable bytesable = Mockito.mock(BackgroundPathAndBytesable.class);
    private final WatchPathable watchPathable = Mockito.mock(WatchPathable.class);
    private final DeleteBuilder deleteBuilder = Mockito.mock(DeleteBuilder.class);

    private final ObjectMapper objectMapper = new ObjectMapper();
    private NewZkSubscriptionClient client;
    private byte[] topology;

    @Before
    public void setUp() throws JsonProcessingException {
        Mockito.when(curatorFrameworkRotator.takeCuratorFramework()).thenReturn(curator);
        Mockito.when(curator.getData()).thenReturn(getDataBuilder);
        Mockito.when(curator.setData()).thenReturn(setDataBuilder);
        Mockito.when(setDataBuilder.withVersion(Mockito.anyInt())).thenReturn(bytesable);
        Mockito.when(getDataBuilder.storingStatIn(Mockito.any())).thenReturn(watchPathable);
        Mockito.when(curator.delete()).thenReturn(deleteBuilder);

        topology = objectMapper.writeValueAsBytes(new ZkSubscriptionClient.Topology(
                new Partition[]{new Partition(
                        "test-event-type", "0", "session-id-xxx", null, Partition.State.REASSIGNING)},
                null
        ));
        client = new NewZkSubscriptionClient(
                "subscription-id-xxx",
                new ZooKeeperHolder.RotatingCuratorFramework(curatorFrameworkRotator),
                objectMapper
        );
    }

    @Test(expected = SubscriptionNotInitializedException.class)
    public void testNoTopologyNodeThenSubscriptionIsNotInited() throws Exception {
        Mockito.when(curator.getData().storingStatIn(Mockito.any()).forPath(Mockito.any()))
                .thenThrow(KeeperException.NoNodeException.class);

        client.transfer("session-id-xxx", null);
    }

    @Test
    public void testTopologyUpdated() throws Exception {
        Mockito.when(curator.getData().storingStatIn(Mockito.any()).forPath(Mockito.any()))
                .thenReturn(topology);
        Mockito.when(curator.getData().forPath(Mockito.any()))
                .thenReturn(topology);
        Mockito.when(curator.setData().withVersion(Mockito.anyInt()).forPath(Mockito.any()))
                .thenReturn(null);

        client.transfer("session-id-xxx", Collections.singleton(
                new EventTypePartition("test-event-type", "0")));
        Mockito.verify(bytesable).forPath(Mockito.anyString(), Mockito.any());
        Mockito.reset(bytesable);
    }

    @Test
    public void testTopologyWasNotUpdated() throws Exception {
        Mockito.when(curator.getData().storingStatIn(Mockito.any()).forPath(Mockito.any()))
                .thenReturn(topology);
        Mockito.when(curator.getData().forPath(Mockito.any()))
                .thenReturn(topology);
        Mockito.when(curator.setData().withVersion(Mockito.anyInt()).forPath(Mockito.any()))
                .thenReturn(null);

        client.transfer("session-id-ANOTHER", Collections.singleton(
                new EventTypePartition("test-event-type", "0")));
        Mockito.verify(bytesable, Mockito.times(0)).forPath(Mockito.anyString(), Mockito.any());
        Mockito.reset(bytesable);
    }

    @Test
    public void whenUnregisterSessionMissingNodeThenOk() throws Exception {

        Mockito.when(curator.delete().forPath(Mockito.any()))
                .thenThrow(KeeperException.NoNodeException.class);

        final Session session = Session.generate(1, ImmutableList.of());

        // It should not throw.
        client.unregisterSession(session);
    }
}
