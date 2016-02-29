package de.zalando.aruha.nakadi.repository.db;

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

import java.util.concurrent.ExecutionException;

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
        String connectString = "127.0.0.1:2181";
        RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 3);
        CuratorFramework cf = CuratorFrameworkFactory.newClient(connectString, retryPolicy);
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
        EventTypeCache etc = new EventTypeCache(dbRepo, client);

        EventType et = buildEventType();

        etc.created(et);

        assertNotNull(client.checkExists().forPath("/nakadi/event_types/" + et.getName()));
    }

    @Test
    public void whenUpdatedSetChildrenZNodeValue() throws Exception {
        EventTypeCache etc = new EventTypeCache(dbRepo, client);

        EventType et = buildEventType();

        client
                .create()
                .creatingParentsIfNeeded()
                .withMode(CreateMode.PERSISTENT)
                .forPath("/nakadi/event_types/" + et.getName(), "some-value".getBytes());

        etc.updated(et.getName());

        byte data[] = client.getData().forPath("/nakadi/event_types/" + et.getName());
        assertThat(data, equalTo(new byte[0]));
    }

    @Test
    public void whenRemovedThenDeleteZNodeValue() throws Exception {
        EventTypeCache etc = new EventTypeCache(dbRepo, client);

        EventType et = buildEventType();

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
        EventTypeCache etc = new EventTypeCache(dbRepo, client);

        EventType et = buildEventType();

        Mockito
                .doReturn(et)
                .when(dbRepo)
                .findByName(et.getName());

        assertThat(etc.get(et.getName()), equalTo(et));
        assertThat(etc.get(et.getName()), equalTo(et));

        verify(dbRepo, times(1)).findByName(et.getName());
    }

    @Test
    public void invalidateCacheOnUpdate() throws Exception {
        EventTypeCache etc = new EventTypeCache(dbRepo, client);

        EventType et = buildEventType();

        Mockito
                .doReturn(et)
                .when(dbRepo)
                .findByName(et.getName());

        etc.created(et);
        etc.get(et.getName());
        etc.updated(et.getName());

        executeWithRetry(() -> {
                    try {
                        etc.get(et.getName());
                        verify(dbRepo, times(2)).findByName(et.getName());
                    } catch (NoSuchEventTypeException e) {
                        fail();
                    } catch (ExecutionException e) {
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
        eventType.setCategory("event-category");
        eventType.setSchema(schema);

        return eventType;
    }
}
