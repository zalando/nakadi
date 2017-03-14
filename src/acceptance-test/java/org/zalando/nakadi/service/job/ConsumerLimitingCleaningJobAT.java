package org.zalando.nakadi.service.job;

import org.apache.curator.framework.CuratorFramework;
import org.apache.zookeeper.KeeperException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.zalando.nakadi.repository.zookeeper.ZooKeeperHolder;
import org.zalando.nakadi.repository.zookeeper.ZooKeeperLockFactory;
import org.zalando.nakadi.service.ConsumerLimitingService;
import org.zalando.nakadi.webservice.BaseAT;
import org.zalando.nakadi.webservice.utils.ZookeeperTestUtils;

import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ConsumerLimitingCleaningJobAT extends BaseAT {

    private static final CuratorFramework CURATOR = ZookeeperTestUtils.createCurator(ZOOKEEPER_URL);

    private ConsumerLimitingCleaningJob cleaningService;

    public ConsumerLimitingCleaningJobAT() {
        final ZooKeeperHolder zkHolder = mock(ZooKeeperHolder.class);
        when(zkHolder.get()).thenReturn(CURATOR);
        final ZooKeeperLockFactory zkLockFactory = new ZooKeeperLockFactory(zkHolder);
        final ConsumerLimitingService limitingService = new ConsumerLimitingService(zkHolder, zkLockFactory, 5);

        final JobWrapperFactory jobWrapperFactory = mock(JobWrapperFactory.class);
        final ExclusiveJobWrapper jobWrapper = DummyJobWrapper.create();
        when(jobWrapperFactory.createExclusiveJobWrapper(any(), anyLong())).thenReturn(jobWrapper);

        cleaningService = new ConsumerLimitingCleaningJob(zkHolder, jobWrapperFactory, limitingService, 0);
    }

    @Before
    public void before() throws Exception {
        deleteConsumersData();

        // "hanging" node
        CURATOR.create().creatingParentsIfNeeded().forPath("/nakadi/consumers/connections/hanging");

        // "normal" node with child
        CURATOR.create().creatingParentsIfNeeded().forPath("/nakadi/consumers/connections/normal/some_child");
    }

    @After
    public void deleteConsumersData() throws Exception {
        try {
            CURATOR.delete().deletingChildrenIfNeeded().forPath("/nakadi/consumers");
        } catch (final KeeperException.NoNodeException e) {
            // this is fine
        }
    }

    @Test
    public void whenCleanThenOk() throws Exception {
        cleaningService.cleanHangingNodes();

        assertThat("the 'hanging' node should be deleted",
                CURATOR.checkExists().forPath("/nakadi/consumers/connections/hanging"),
                nullValue());
        assertThat("node with children nodes should not be deleted",
                CURATOR.checkExists().forPath("/nakadi/consumers/connections/normal"),
                not(nullValue()));
    }

}
