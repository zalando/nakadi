package org.zalando.nakadi.repository.db;

import com.google.common.collect.ImmutableList;
import java.io.Closeable;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;
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
import org.mockito.Matchers;
import org.mockito.Mockito;
import org.zalando.nakadi.config.RepositoriesConfig;
import org.zalando.nakadi.domain.EventType;
import org.zalando.nakadi.domain.Storage;
import org.zalando.nakadi.domain.Timeline;
import org.zalando.nakadi.repository.EventTypeRepository;
import org.zalando.nakadi.repository.zookeeper.ZooKeeperHolder;
import org.zalando.nakadi.service.timeline.TimelineSync;
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
    private final TimelineSync timelineSync;

    public EventTypeCacheTestAT() throws Exception {
        final String connectString = "127.0.0.1:2181";
        final RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 3);
        final CuratorFramework cf = CuratorFrameworkFactory.newClient(connectString, retryPolicy);
        cf.start();
        this.client = Mockito.mock(ZooKeeperHolder.class);
        Mockito.when(this.client.get()).thenReturn(cf);
        this.timelineSync = Mockito.mock(TimelineSync.class);
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
        final EventTypeCache etc = new EventTypeCache(eventTypeRepository, timelineRepository, client, timelineSync);
        final EventType et = buildDefaultEventType();

        etc.created(et.getName());

        assertNotNull(client.get().checkExists().forPath("/nakadi/event_types/" + et.getName()));
    }

    @Test
    public void whenUpdatedSetChildrenZNodeValue() throws Exception {
        final EventTypeCache etc = new EventTypeCache(eventTypeRepository, timelineRepository, client, timelineSync);
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
        final EventTypeCache etc = new EventTypeCache(eventTypeRepository, timelineRepository, client, timelineSync);
        final EventType et = buildDefaultEventType();
        Mockito.when(timelineSync.registerTimelineChangeListener(Matchers.eq(et.getName()), Mockito.any()))
                .thenReturn(() -> {});
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
        final EventTypeCache etc = new EventTypeCache(eventTypeRepository, timelineRepository, client, timelineSync);
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
        final EventTypeCache etc = new RepositoriesConfig()
                .eventTypeCache(client, eventTypeRepository, timelineRepository, timelineSync);
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
        final EventTypeCache etc = new RepositoriesConfig()
                .eventTypeCache(client, eventTypeRepository, timelineRepository, timelineSync);
        final EventType et = buildDefaultEventType();

        Mockito.when(timelineRepository.listTimelinesOrdered(et.getName()))
                .thenReturn(getMockedTimelines(et.getName()));
        Mockito.doReturn(et).when(eventTypeRepository).findByName(et.getName());

        etc.created(et.getName());
        final Optional<Timeline> timeline = etc.getActiveTimeline(et.getName());
        Assert.assertEquals(1, timeline.get().getOrder());
    }

    @Test
    public void testGetTimelines() throws Exception {
        final EventTypeCache etc = new RepositoriesConfig()
                .eventTypeCache(client, eventTypeRepository, timelineRepository, timelineSync);
        final EventType et = buildDefaultEventType();

        Mockito.when(timelineRepository.listTimelinesOrdered(et.getName()))
                .thenReturn(getMockedTimelines(et.getName()));
        Mockito.doReturn(et).when(eventTypeRepository).findByName(et.getName());

        final List<Timeline> timelines = etc.getTimelinesOrdered(et.getName());
        Assert.assertEquals(3, timelines.size());
    }

    @Test
    public void invalidateCacheOnTimelineChange() throws Exception {
        final TestTimelineSync timelineSync = new TestTimelineSync();
        final EventTypeCache etc = new RepositoriesConfig()
                .eventTypeCache(client, eventTypeRepository, timelineRepository, timelineSync);
        final EventType et = buildDefaultEventType();

        Mockito.when(timelineRepository.listTimelinesOrdered(et.getName()))
                .thenReturn(getMockedTimelines(et.getName()));
        Mockito.doReturn(et).when(eventTypeRepository).findByName(et.getName());

        etc.created(et.getName());
        timelineSync.invokeListeners();

        executeWithRetry(() -> {
                    try {
                        etc.getTimelinesOrdered(et.getName());
                        verify(timelineRepository, times(1)).listTimelinesOrdered(et.getName());
                    } catch (final Exception e) {
                        fail();
                    }
                },
                new RetryForSpecifiedTimeStrategy<Void>(5000).withExceptionsThatForceRetry(AssertionError.class)
                        .withWaitBetweenEachTry(500));
    }

    private List<Timeline> getMockedTimelines(final String etName) {
        final Timeline t1 = new Timeline(etName, 0, new Storage(), "topic", new Date());
        final Timeline t2 = new Timeline(etName, 1, new Storage(), "topic", new Date(System.currentTimeMillis() + 200));
        t2.setSwitchedAt(new Date(System.currentTimeMillis() + 300));
        final Timeline t3 = new Timeline(etName, 2, new Storage(), "topic", new Date(System.currentTimeMillis() + 500));
        return ImmutableList.of(t1, t2, t3);
    }

    private class TestTimelineSync implements TimelineSync {


        private Map<String, Consumer<String>> listeners = new HashMap<>();

        @Override
        public Closeable workWithEventType(final String eventType, final long timeoutMs)
                throws InterruptedException, TimeoutException {
            return null;
        }

        @Override
        public void startTimelineUpdate(final String eventType, final long timeoutMs)
                throws InterruptedException, IllegalStateException {
            // stub for test purpose
        }

        @Override
        public void finishTimelineUpdate(final String eventType) throws InterruptedException {
            // stub for test purpose
        }

        @Override
        public ListenerRegistration registerTimelineChangeListener(final String eventType,
                                                                   final Consumer<String> listener) {
            listeners.put(eventType, listener);
            return () -> {};
        }

        public void invokeListeners() {
            listeners.forEach((etName, listener) -> listener.accept(etName));
        }
    }
}
