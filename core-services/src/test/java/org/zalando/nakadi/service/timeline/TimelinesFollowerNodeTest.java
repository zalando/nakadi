package org.zalando.nakadi.service.timeline;

import com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.zalando.nakadi.utils.TestUtils;

import java.util.Collections;
import java.util.Objects;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class TimelinesFollowerNodeTest {
    private TimelinesZookeeper timelinesZookeeper;
    private LocalLockIntegration localLockIntegration;
    private TimelinesFollowerNode node;
    private Watcher watcherRegistration;
    private Long startVersion;
    private Integer zkStartVersion;
    private static final WatchedEvent EVT = new WatchedEvent(
            Watcher.Event.EventType.NodeDataChanged, Watcher.Event.KeeperState.SyncConnected, "/doesn't matter");

    @Before
    public void before() throws JsonProcessingException, InterruptedException {
        timelinesZookeeper = Mockito.mock(TimelinesZookeeper.class);
        localLockIntegration = Mockito.mock(LocalLockIntegration.class);

        final ArgumentCaptor<Watcher> watcherArgumentCaptor = ArgumentCaptor.forClass(Watcher.class);
        startVersion = 100L;
        zkStartVersion = 123;
        when(timelinesZookeeper.getCurrentState(watcherArgumentCaptor.capture()))
                .thenReturn(new TimelinesZookeeper.ZkVersionedLockedEventTypes(
                        new VersionedLockedEventTypes(
                                startVersion, Collections.emptySet()
                        ),
                        zkStartVersion));

        node = new TimelinesFollowerNode(timelinesZookeeper, localLockIntegration);
        node.initializeNode();
        // Wait for node to get very first version
        TestUtils.waitFor(() -> {
            try {
                // wait for first real update
                verify(timelinesZookeeper, times(1)).exposeSelfVersion(startVersion);
            } catch (InterruptedException e) {
                throw new AssertionError(e);
            }
        }, 500, 100);
        watcherRegistration = watcherArgumentCaptor.getAllValues().stream().filter(Objects::nonNull).findAny().get();
    }

    @After
    public void after() {
        node.destroyNode();
        node = null;
    }

    @Test(timeout = 5_000)
    public void testUpdatesArePropagated() throws InterruptedException {
        final VersionedLockedEventTypes updated = new VersionedLockedEventTypes(
                startVersion + 1,
                Collections.singleton("test")
        );

        when(timelinesZookeeper.getCurrentState(any())).thenReturn(
                new TimelinesZookeeper.ZkVersionedLockedEventTypes(updated, zkStartVersion + 1));
        // trigger zookeeper notification manually.
        watcherRegistration.process(EVT);
        // wait for local lock manager to be called.
        TestUtils.waitFor(() -> {
                    try {
                        verify(localLockIntegration, times(1)).setLockedEventTypes(eq(Collections.singleton("test")));
                    } catch (InterruptedException e) {
                        throw new AssertionError("");
                    }
                },
                500, 100
        );
        verify(timelinesZookeeper, times(1)).exposeSelfVersion(eq(updated.getVersion()));
    }
}