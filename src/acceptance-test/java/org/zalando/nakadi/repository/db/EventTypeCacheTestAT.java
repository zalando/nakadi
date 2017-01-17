package org.zalando.nakadi.repository.db;

import com.google.common.collect.ImmutableList;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.CreateMode;
import org.echocat.jomon.runtime.concurrent.RetryForSpecifiedTimeStrategy;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.zalando.nakadi.config.RepositoriesConfig;
import org.zalando.nakadi.domain.EventType;
import org.zalando.nakadi.repository.EventTypeRepository;
import org.zalando.nakadi.repository.zookeeper.ZooKeeperHolder;
import org.zalando.nakadi.domain.EventType;
import org.zalando.nakadi.domain.Storage;
import org.zalando.nakadi.domain.Timeline;
import org.zalando.nakadi.repository.EventTypeRepository;

import java.util.Date;
import java.util.List;

import static junit.framework.Assert.assertNotNull;
import static junit.framework.Assert.assertNull;
import static junit.framework.Assert.fail;
import static org.echocat.jomon.runtime.concurrent.Retryer.executeWithRetry;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.zalando.nakadi.utils.TestUtils.buildDefaultEventType;

public class EventTypeCacheTestAT {

    private final EventTypeRepository eventTypeRepository = mock(EventTypeRepository.class);
    private final TimelineDbRepository timelineRepository = mock(TimelineDbRepository.class);
    private final ZooKeeperHolder client;

    public EventTypeCacheTestAT() throws Exception {
        final String connectString = "127.0.0.1:2181";
        final RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 3);
        final CuratorFramework cf = CuratorFrameworkFactory.newClient(connectString, retryPolicy);
        cf.start();
        this.client = Mockito.mock(ZooKeeperHolder.class);
        Mockito.when(this.client.get()).thenReturn(cf);
    }

    @Before
    @After
    public void setUp() throws Exception {
        if (client.get().checkExists().forPath("/nakadi/event_types/event-name") != null) {
            client.get().delete().forPath("/nakadi/event_types/event-name");
        }
    }

    @Test
    public void onCreatedAddNewChildrenZNode() throws Exception {
        final EventTypeCache etc = new EventTypeCache(eventTypeRepository, timelineRepository, client);

        final EventType et = buildDefaultEventType();

        etc.created(et.getName());

        assertNotNull(client.get().checkExists().forPath("/nakadi/event_types/" + et.getName()));
    }

    @Test
    public void whenUpdatedSetChildrenZNodeValue() throws Exception {
        final EventTypeCache etc = new EventTypeCache(eventTypeRepository, timelineRepository, client);

        final EventType et = buildDefaultEventType();

        client.get()
                .create()
                .creatingParentsIfNeeded()
                .withMode(CreateMode.PERSISTENT)
                .forPath("/nakadi/event_types/" + et.getName(), "some-value".getBytes());

        etc.updated(et.getName());

        final byte data[] = client.get().getData().forPath("/nakadi/event_types/" + et.getName());
        assertThat(data, equalTo(new byte[0]));
    }

    @Test
    public void whenRemovedThenDeleteZNodeValue() throws Exception {
        final EventTypeCache etc = new EventTypeCache(eventTypeRepository, timelineRepository, client);

        final EventType et = buildDefaultEventType();

        client.get()
                .create()
                .creatingParentsIfNeeded()
                .withMode(CreateMode.PERSISTENT)
                .forPath("/nakadi/event_types/" + et.getName());

        etc.removed(et.getName());

        assertNull(client.get().checkExists().forPath("/nakadi/event_types/" + et.getName()));
    }

    @Test
    public void loadsFromDbOnCacheMissTest() throws Exception {
        final EventTypeCache etc = new EventTypeCache(eventTypeRepository, timelineRepository, client);

        final EventType et = buildDefaultEventType();

        Mockito
                .doReturn(et)
                .when(eventTypeRepository)
                .findByName(et.getName());

        assertThat(etc.getEventType(et.getName()), equalTo(et));
        assertThat(etc.getEventType(et.getName()), equalTo(et));

        verify(eventTypeRepository, times(1)).findByName(et.getName());
    }

    @SuppressWarnings("unchecked")
    @Test
    public void invalidateCacheOnUpdate() throws Exception {
        final EventTypeCache etc = new RepositoriesConfig().eventTypeCache(client, eventTypeRepository);

        final EventType et = buildDefaultEventType();

        Mockito
                .doReturn(et)
                .when(eventTypeRepository)
                .findByName(et.getName());

        etc.created(et.getName());
        etc.getEventType(et.getName());
        etc.updated(et.getName());

        executeWithRetry(() -> {
                    try {
                        etc.getEventType(et.getName());
                        verify(eventTypeRepository, times(2)).findByName(et.getName());
                    } catch (final Exception e) {
                        fail();
                    }
                },
                new RetryForSpecifiedTimeStrategy<Void>(5000).withExceptionsThatForceRetry(AssertionError.class)
                        .withWaitBetweenEachTry(500));

    }

    @Test
    public void testGetActiveTimeline() throws Exception {
        Mockito.when(timelineRepository.listTimelines("test")).thenReturn(getMockedTimelines());
        final EventTypeCache etc = new EventTypeCache(eventTypeRepository, timelineRepository, client);
        final Timeline timeline = etc.getActiveTimeline("test");
        Assert.assertEquals(Integer.valueOf(1), timeline.getOrder());
    }

    @Test
    public void testGetTimelines() throws Exception {
        Mockito.when(timelineRepository.listTimelines("test")).thenReturn(getMockedTimelines());
        final EventTypeCache etc = new EventTypeCache(eventTypeRepository, timelineRepository, client);
        final List<Timeline> timelines = etc.getTimelines("test");
        Assert.assertEquals(3, timelines.size());
    }

    public List<Timeline> getMockedTimelines() {
        final Timeline t1 = new Timeline("test", 0, new Storage(), "topic", new Date());
        final Timeline t2 = new Timeline("test", 1, new Storage(), "topic", new Date(System.currentTimeMillis() + 200));
        t2.setSwitchedAt(new Date(System.currentTimeMillis() + 300));
        final Timeline t3 = new Timeline("test", 2, new Storage(), "topic", new Date(System.currentTimeMillis() + 500));
        final Timeline t4 = new Timeline("test2", 3, new Storage(), "topic2", new Date(System.currentTimeMillis() +
                700));
        return ImmutableList.of(t1, t2, t3, t4);
    }
}
