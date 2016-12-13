package org.zalando.nakadi.webservice;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.curator.framework.CuratorFramework;
import org.apache.zookeeper.KeeperException;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.zalando.nakadi.config.JsonConfig;
import org.zalando.nakadi.repository.zookeeper.ZooKeeperHolder;
import org.zalando.nakadi.repository.zookeeper.ZooKeeperLockFactory;
import org.zalando.nakadi.service.ConsumerLimitingCleaningService;
import org.zalando.nakadi.service.ConsumerLimitingService;
import org.zalando.nakadi.webservice.utils.ZookeeperTestUtils;

import java.nio.charset.Charset;

import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.when;

public class ConsumerLimitingCleaningServiceAT extends BaseAT {

    private static final CuratorFramework CURATOR = ZookeeperTestUtils.createCurator(ZOOKEEPER_URL);

    private ConsumerLimitingCleaningService cleaningService;
    private ObjectMapper objectMapper;

    public ConsumerLimitingCleaningServiceAT() {
        final ZooKeeperHolder zkHolder = Mockito.mock(ZooKeeperHolder.class);
        when(zkHolder.get()).thenReturn(CURATOR);
        final ZooKeeperLockFactory zkLockFactory = new ZooKeeperLockFactory(zkHolder);
        final ConsumerLimitingService limitingService = new ConsumerLimitingService(zkHolder, zkLockFactory, 5);

        objectMapper = new JsonConfig().jacksonObjectMapper();
        cleaningService = new ConsumerLimitingCleaningService(zkHolder, objectMapper, limitingService);
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
    public void whenItsNotTimeToRunCleaningThenNothingIsCleaned() throws Exception {
        // set latest cleaning as two hours ago
        createLatestCleanNode(2);

        cleaningService.cleanHangingNodes();

        assertThat("the 'hanging' node should not be deleted",
                CURATOR.checkExists().forPath("/nakadi/consumers/connections/hanging"),
                not(nullValue()));
        assertThat("node with children nodes should not be deleted",
                CURATOR.checkExists().forPath("/nakadi/consumers/connections/normal"),
                not(nullValue()));
    }

    @Test
    public void whenCleaningAlreadyPerformedOnAnotherNodeThenNothingIsCleaned() throws Exception {
        // set latest cleaning as 30 hours ago
        createLatestCleanNode(30);

        // another Nakadi instance already performs cleaning
        CURATOR.create().creatingParentsIfNeeded().forPath("/nakadi/consumers/cleaning/lock");

        cleaningService.cleanHangingNodes();

        assertThat("the 'hanging' node should not be deleted",
                CURATOR.checkExists().forPath("/nakadi/consumers/connections/hanging"),
                not(nullValue()));
        assertThat("node with children nodes should not be deleted",
                CURATOR.checkExists().forPath("/nakadi/consumers/connections/normal"),
                not(nullValue()));
    }

    @Test
    public void whenCleaningThenOk() throws Exception {
        // set latest cleaning as 30 hours ago
        createLatestCleanNode(30);

        cleaningService.cleanHangingNodes();

        assertThat("the 'hanging' node should be deleted",
                CURATOR.checkExists().forPath("/nakadi/consumers/connections/hanging"),
                nullValue());
        assertThat("node with children nodes should not be deleted",
                CURATOR.checkExists().forPath("/nakadi/consumers/connections/normal"),
                not(nullValue()));
    }

    private void createLatestCleanNode(final int hoursAgo) throws Exception {
        final DateTime now = new DateTime(DateTimeZone.UTC);
        final DateTime twoHoursAgo = now.minusHours(hoursAgo);
        final byte[] data = objectMapper.writeValueAsString(twoHoursAgo).getBytes(Charset.forName("UTF-8"));
        CURATOR.create().creatingParentsIfNeeded().forPath("/nakadi/consumers/cleaning/latest", data);
    }

}
