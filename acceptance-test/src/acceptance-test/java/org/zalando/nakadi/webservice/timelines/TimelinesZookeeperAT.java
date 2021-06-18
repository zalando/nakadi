package org.zalando.nakadi.webservice.timelines;

import com.google.common.base.Charsets;
import org.apache.curator.framework.CuratorFramework;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.zalando.nakadi.repository.zookeeper.ZooKeeperHolder;
import org.zalando.nakadi.service.timeline.TimelinesZookeeper;
import org.zalando.nakadi.service.timeline.VersionedLockedEventTypes;
import org.zalando.nakadi.util.UUIDGenerator;
import org.zalando.nakadi.webservice.BaseAT;
import org.zalando.nakadi.webservice.utils.ZookeeperTestUtils;

import java.util.UUID;

public class TimelinesZookeeperAT extends BaseAT {

    private static final CuratorFramework CURATOR = ZookeeperTestUtils.createCurator(ZOOKEEPER_URL);

    private String nodePath;
    private TimelinesZookeeper timelinesZookeeper;

    @Before
    public void prepareNodeId() {
        final UUID nodeId = UUID.randomUUID();
        final UUIDGenerator uuidGenerator = Mockito.mock(UUIDGenerator.class);
        Mockito.when(uuidGenerator.randomUUID()).thenReturn(nodeId);
        final ZooKeeperHolder zookeeperHolder = Mockito.mock(ZooKeeperHolder.class);
        Mockito.when(zookeeperHolder.get()).thenReturn(CURATOR);

        timelinesZookeeper = new TimelinesZookeeper(zookeeperHolder, uuidGenerator, MAPPER);
        nodePath = "/nakadi/timelines/nodes/" + nodeId;
    }

    @After
    public void cleanNodeId() throws Exception {
        CURATOR.delete().forPath(nodePath);
    }

    @Test
    public void testNodeInformationWrittenOnStart() throws Exception {

        timelinesZookeeper.prepareZookeeperStructure();

        timelinesZookeeper.exposeSelfVersion(VersionedLockedEventTypes.EMPTY.getVersion());
        Assert.assertEquals(VersionedLockedEventTypes.EMPTY.getVersion().toString(),
                new String(CURATOR.getData().forPath(nodePath), Charsets.UTF_8));
    }

}