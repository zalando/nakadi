package de.zalando.aruha.nakadi.repository.db;

import de.zalando.aruha.nakadi.domain.EventCategory;
import de.zalando.aruha.nakadi.domain.EventType;
import de.zalando.aruha.nakadi.domain.EventTypeSchema;
import de.zalando.aruha.nakadi.exceptions.NoSuchEventTypeException;
import de.zalando.aruha.nakadi.repository.EventTypeRepository;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.CreateMode;
import org.echocat.jomon.runtime.concurrent.RetryForSpecifiedTimeStrategy;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import static junit.framework.Assert.assertNotNull;
import static junit.framework.Assert.assertNull;
import static junit.framework.Assert.fail;
import static org.echocat.jomon.runtime.concurrent.Retryer.executeWithRetry;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class EventTypeCacheTest {

    private final EventTypeRepository dbRepo = mock(EventTypeRepository.class);
    private final CuratorFramework client;

    public EventTypeCacheTest() throws Exception {
        final String connectString = "127.0.0.1:2181";
        final RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 3);
        final CuratorFramework cf = CuratorFrameworkFactory.newClient(connectString, retryPolicy);
        cf.start();
        this.client = cf;
    }

    @Before
    @After
    public void setUp() throws Exception {
        if (client.checkExists().forPath("/nakadi/event_types/event-name") != null) {
            client.delete().forPath("/nakadi/event_types/event-name");
        }
    }

    @Test
    public void onCreatedAddNewChildrenZNode() throws Exception {
        final EventTypeCache etc = new EventTypeCache(dbRepo, client);

        final EventType et = buildEventType();

        etc.created(et.getName());

        assertNotNull(client.checkExists().forPath("/nakadi/event_types/" + et.getName()));
    }

    @Test
    public void whenUpdatedSetChildrenZNodeValue() throws Exception {
        final EventTypeCache etc = new EventTypeCache(dbRepo, client);

        final EventType et = buildEventType();

        client
                .create()
                .creatingParentsIfNeeded()
                .withMode(CreateMode.PERSISTENT)
                .forPath("/nakadi/event_types/" + et.getName(), "some-value".getBytes());

        etc.updated(et.getName());

        final byte data[] = client.getData().forPath("/nakadi/event_types/" + et.getName());
        assertThat(data, equalTo(new byte[0]));
    }

    @Test
    public void whenRemovedThenDeleteZNodeValue() throws Exception {
        final EventTypeCache etc = new EventTypeCache(dbRepo, client);

        final EventType et = buildEventType();

        client
                .create()
                .creatingParentsIfNeeded()
                .withMode(CreateMode.PERSISTENT)
                .forPath("/nakadi/event_types/" + et.getName());

        etc.removed(et.getName());

        assertNull(client.checkExists().forPath("/nakadi/event_types/" + et.getName()));
    }

    @Test
    public void loadsFromDbOnCacheMissTest() throws Exception {
        final EventTypeCache etc = new EventTypeCache(dbRepo, client);

        final EventType et = buildEventType();

        Mockito
                .doReturn(et)
                .when(dbRepo)
                .findByName(et.getName());

        assertThat(etc.getEventType(et.getName()), equalTo(et));
        assertThat(etc.getEventType(et.getName()), equalTo(et));

        verify(dbRepo, times(1)).findByName(et.getName());
    }

    @SuppressWarnings("unchecked")
    @Test
    public void invalidateCacheOnUpdate() throws Exception {
        final EventTypeCache etc = new EventTypeCache(dbRepo, client);

        final EventType et = buildEventType();

        Mockito
                .doReturn(et)
                .when(dbRepo)
                .findByName(et.getName());

        etc.created(et.getName());
        etc.getEventType(et.getName());
        etc.updated(et.getName());

        executeWithRetry(() -> {
                    try {
                        etc.getEventType(et.getName());
                        verify(dbRepo, times(2)).findByName(et.getName());
                    } catch (NoSuchEventTypeException e) {
                        fail();
                    } catch (Exception e) {
                        fail();
                    }
                },
                new RetryForSpecifiedTimeStrategy<Void>(5000).withExceptionsThatForceRetry(AssertionError.class)
                        .withWaitBetweenEachTry(500));

    }

    private EventType buildEventType() {
        final EventTypeSchema schema = new EventTypeSchema();
        final EventType eventType = new EventType();

        schema.setSchema("{ \"price\": 1000 }");
        schema.setType(EventTypeSchema.Type.JSON_SCHEMA);

        eventType.setName("event-name");
        eventType.setCategory(EventCategory.BUSINESS);
        eventType.setSchema(schema);

        return eventType;
    }
}
