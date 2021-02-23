package org.zalando.nakadi.service.timeline;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

public class TimelineSyncImplTest {

    private TimelinesZookeeper timelinesZookeeper;
    private LocalLockManager localLockManager;

    private TimelineSyncImpl timelineSync;

    @Before
    public void beforeTest() {
        timelinesZookeeper = Mockito.mock(TimelinesZookeeper.class);
        localLockManager = Mockito.mock(LocalLockManager.class);

        timelineSync = new TimelineSyncImpl(timelinesZookeeper, localLockManager);
    }

    @Test
    public void whenStartTimelineUpdateThenVersionUpdated() throws InterruptedException {
    }

    @Test
    public void whenStartTimelineUpdateThenSeveralAttemptsMade() throws InterruptedException {
    }

    @Test
    public void whenStartTimelineUpdateThenWaitForAllNodes() {
    }

    @Test
    public void whenStartTimelineUpdateTimeoutThanLockedRolledBack() {
    }
}