package org.zalando.nakadi.service.timeline;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.Collections;
import java.util.Map;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class TimelineSyncImplTest {

    private TimelinesZookeeper timelinesZookeeper;
    private TimelineSyncImpl timelineSync;

    @Before
    public void beforeTest() {
        timelinesZookeeper = Mockito.mock(TimelinesZookeeper.class);

        timelineSync = new TimelineSyncImpl(timelinesZookeeper, Mockito.mock(LocalLockManager.class));
    }

    @Test(timeout = 2_000)
    public void whenStartTimelineUpdateThenVersionUpdated() throws InterruptedException {
        when(timelinesZookeeper.getCurrentVersion(any())).thenReturn(
                new TimelinesZookeeper.ZkVersionedLockedEventTypes(VersionedLockedEventTypes.EMPTY, 123));
        final VersionedLockedEventTypes expectedVersion = new VersionedLockedEventTypes(
                VersionedLockedEventTypes.EMPTY.getVersion() + 1, Collections.singleton("test"));
        when(timelinesZookeeper.setCurrentVersion(eq(expectedVersion), eq(123)))
                .thenReturn(true);

        timelineSync.startTimelineUpdate("test", 1000);
    }

    @Test(timeout = 2_000)
    public void whenStartTimelineUpdateThenSeveralAttemptsMade() throws InterruptedException {
        when(timelinesZookeeper.getCurrentVersion(any())).thenReturn(
                new TimelinesZookeeper.ZkVersionedLockedEventTypes(VersionedLockedEventTypes.EMPTY, 123),
                new TimelinesZookeeper.ZkVersionedLockedEventTypes(VersionedLockedEventTypes.EMPTY, 124));

        final VersionedLockedEventTypes expectedVersion = new VersionedLockedEventTypes(
                VersionedLockedEventTypes.EMPTY.getVersion() + 1, Collections.singleton("test"));

        when(timelinesZookeeper.setCurrentVersion(eq(expectedVersion), eq(123))).thenReturn(false);
        when(timelinesZookeeper.setCurrentVersion(eq(expectedVersion), eq(124))).thenReturn(true);

        timelineSync.startTimelineUpdate("test", 1000);

        verify(timelinesZookeeper, times(1)).setCurrentVersion(eq(expectedVersion), eq(123));
        verify(timelinesZookeeper, times(1)).setCurrentVersion(eq(expectedVersion), eq(124));
    }

    @Test(timeout = 2_000)
    public void whenStartTimelineUpdateThenWaitForAllNodes() throws InterruptedException {
        when(timelinesZookeeper.getCurrentVersion(any())).thenReturn(
                new TimelinesZookeeper.ZkVersionedLockedEventTypes(VersionedLockedEventTypes.EMPTY, 123));

        final VersionedLockedEventTypes expectedVersion = new VersionedLockedEventTypes(
                VersionedLockedEventTypes.EMPTY.getVersion() + 1, Collections.singleton("test"));
        when(timelinesZookeeper.setCurrentVersion(eq(expectedVersion), eq(123))).thenReturn(true);

        final Map<String, Long> outdated = Collections.singletonMap(
                "xxx", VersionedLockedEventTypes.EMPTY.getVersion());
        final Map<String, Long> uptodate = Collections.singletonMap(
                "xxx", VersionedLockedEventTypes.EMPTY.getVersion() + 1);

        when(timelinesZookeeper.getNodesVersions()).thenReturn(outdated, outdated, uptodate);

        timelineSync.startTimelineUpdate("test", 1000);

        verify(timelinesZookeeper, times(3)).getNodesVersions();
    }

    @Test(timeout = 2_000)
    public void whenStartTimelineUpdateFailThanLockedRolledBack() throws InterruptedException {
        final VersionedLockedEventTypes expectedVersion = new VersionedLockedEventTypes(
                VersionedLockedEventTypes.EMPTY.getVersion() + 1, Collections.singleton("test"));
        final VersionedLockedEventTypes expectedRollback = new VersionedLockedEventTypes(
                expectedVersion.getVersion() + 1, Collections.emptySet());
        when(timelinesZookeeper.getCurrentVersion(any())).thenReturn(
                new TimelinesZookeeper.ZkVersionedLockedEventTypes(VersionedLockedEventTypes.EMPTY, 123),
                new TimelinesZookeeper.ZkVersionedLockedEventTypes(expectedVersion, 124));

        when(timelinesZookeeper.setCurrentVersion(eq(expectedVersion), eq(123))).thenReturn(true);
        when(timelinesZookeeper.setCurrentVersion(eq(expectedRollback), eq(124))).thenReturn(true);
        when(timelinesZookeeper.getNodesVersions()).thenThrow(new RuntimeException("timeout or some exception"));

        try {
            timelineSync.startTimelineUpdate("test", 1000);
            Assert.fail("Expected exception to be thrown");
        } catch (RuntimeException ex) {
            verify(timelinesZookeeper, times(1)).setCurrentVersion(eq(expectedVersion), eq(123));
            verify(timelinesZookeeper, times(1)).setCurrentVersion(eq(expectedRollback), eq(124));
        }
    }
}