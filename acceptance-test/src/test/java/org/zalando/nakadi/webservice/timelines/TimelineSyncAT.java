package org.zalando.nakadi.webservice.timelines;

import org.apache.curator.framework.CuratorFramework;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;
import org.zalando.nakadi.repository.zookeeper.ZooKeeperHolder;
import org.zalando.nakadi.service.timeline.TimelineSync;
import org.zalando.nakadi.service.timeline.TimelineSyncImpl;
import org.zalando.nakadi.util.ThreadUtils;
import org.zalando.nakadi.util.UUIDGenerator;
import org.zalando.nakadi.utils.TestUtils;
import org.zalando.nakadi.webservice.BaseAT;
import org.zalando.nakadi.webservice.utils.ZookeeperTestUtils;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

public class TimelineSyncAT extends BaseAT {
    private static final CuratorFramework CURATOR = ZookeeperTestUtils.createCurator(ZOOKEEPER_URL);
    private static final ArrayList<Runnable> DELAYED_RUNS = new ArrayList<>();
    private static Thread delayedRunsExecutor;
    private UUIDGenerator uuidGenerator;
    private ZooKeeperHolder zookeeperHolder;

    private TimelineSyncImpl createTimelineSync() throws InterruptedException {
        if (null == uuidGenerator) {
            uuidGenerator = new UUIDGenerator();
        }
        if (null == zookeeperHolder) {
            zookeeperHolder = Mockito.mock(ZooKeeperHolder.class);
            Mockito.when(zookeeperHolder.get()).thenReturn(CURATOR);
        }
        final TimelineSyncImpl result = new TimelineSyncImpl(zookeeperHolder, uuidGenerator);
        synchronized (DELAYED_RUNS) {
            DELAYED_RUNS.add(() -> {
                try {
                    result.reactOnEventTypesChange();
                } catch (final InterruptedException ex) {
                    Thread.currentThread().interrupt();
                }
            });
        }
        return result;
    }

    @BeforeClass
    public static void initialize() {
        delayedRunsExecutor = new Thread(() -> {
            while (true) {
                try {
                    ThreadUtils.sleep(300L);
                    synchronized (DELAYED_RUNS) {
                        DELAYED_RUNS.forEach(Runnable::run);
                    }
                } catch (final InterruptedException e) {
                    break;
                }
            }
        }, "AT_delayed_runs");
        delayedRunsExecutor.setDaemon(true);
        delayedRunsExecutor.start();
    }

    @AfterClass
    public static void terminate() {
        delayedRunsExecutor.interrupt();
        delayedRunsExecutor = null;
        CURATOR.close();
    }

    @Test
    public void testNodeInformationWrittenOnStart() throws Exception {
        final TimelineSyncImpl sync = createTimelineSync();
        final String currentVersion = new String(CURATOR.getData().forPath("/nakadi/timelines/version"));
        Assert.assertEquals(currentVersion,
                new String(CURATOR.getData().forPath("/nakadi/timelines/nodes/" + sync.getNodeId())));
    }

    @Test
    public void testTimelineUpdateWaitsForActivePublish() throws InterruptedException, IOException, TimeoutException {
        final TimelineSync t1 = createTimelineSync();
        final TimelineSync t2 = createTimelineSync();
        final String eventType = UUID.randomUUID().toString();
        final AtomicBoolean updated = new AtomicBoolean(false);

        try (Closeable ignored = t1.workWithEventType(eventType, TimeUnit.SECONDS.toMillis(1))) {
            new Thread(() -> {
                try {
                    t2.startTimelineUpdate(eventType, TimeUnit.SECONDS.toMillis(30));
                    updated.set(true);
                } catch (final InterruptedException ex) {
                    throw new RuntimeException(ex);
                }
            }, "timeline_update").start();
            // Wait a little bit for thread to start timeline update
            ThreadUtils.sleep(TimeUnit.SECONDS.toMillis(3));
            Assert.assertEquals(false, updated.get());
        }
        TestUtils.waitFor(() -> Assert.assertTrue(updated.get()));
    }

    @Test
    public void testPublishPauseOnTimelineUpdate() throws InterruptedException, IOException {
        final TimelineSync t1 = createTimelineSync();
        final TimelineSync t2 = createTimelineSync();
        final String eventType = UUID.randomUUID().toString();
        // Lock publishing
        t1.startTimelineUpdate(eventType, TimeUnit.SECONDS.toMillis(30));
        final AtomicBoolean lockTaken = new AtomicBoolean(false);
        new Thread(() -> {
            try (Closeable cl = t2.workWithEventType(eventType, TimeUnit.SECONDS.toMillis(5))) {
                lockTaken.set(true);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }, "publisher").start();

        ThreadUtils.sleep(TimeUnit.SECONDS.toMillis(2)); // Wait for thread to start.
        Assert.assertEquals(false, lockTaken.get());

        // Now release event type
        t1.finishTimelineUpdate(eventType);
        Assert.assertEquals(true, lockTaken.get());
    }

    private static class RefreshListener {
        private final Map<String, Integer> data;

        private RefreshListener() {
            this.data = new HashMap<>();
        }

        public Map<String, Integer> getData() {
            return data;
        }

        public Consumer<String> createConsumer() {
            return this::accept;
        }

        public void accept(final String s) {
            if (!data.containsKey(s)) {
                data.put(s, 1);
            } else {
                data.put(s, data.get(s) + 1);
            }
        }
    }

    @Test
    public void testNotificationsCalledWhenTimelineUpdated() throws InterruptedException {
        final String eventType1 = UUID.randomUUID().toString();
        final String eventType2 = UUID.randomUUID().toString();

        final RefreshListener l1 = new RefreshListener();
        final TimelineSync t1 = createTimelineSync();
        t1.registerTimelineChangeListener(eventType1, l1.createConsumer());

        final RefreshListener l2 = new RefreshListener();
        final TimelineSync t2 = createTimelineSync();
        t2.registerTimelineChangeListener(eventType2, l2.createConsumer());

        t1.startTimelineUpdate(eventType1, TimeUnit.SECONDS.toMillis(3));
        t1.startTimelineUpdate(eventType2, TimeUnit.SECONDS.toMillis(3));

        Assert.assertEquals(0, l1.getData().size());
        Assert.assertEquals(0, l2.getData().size());

        t1.finishTimelineUpdate(eventType1);
        // Wait for listeners
        TestUtils.waitFor(() -> Assert.assertEquals(1, l1.getData().size()), TimeUnit.SECONDS.toMillis(1));
        Assert.assertEquals(new Integer(1), l1.getData().get(eventType1));
        Assert.assertEquals(0, l2.getData().size());

        t1.finishTimelineUpdate(eventType2);
        Assert.assertEquals(1, l1.getData().size());
        Assert.assertEquals(new Integer(1), l1.getData().get(eventType1));
        Assert.assertEquals(1, l2.getData().size());
        Assert.assertEquals(new Integer(1), l2.getData().get(eventType2));
    }
}
