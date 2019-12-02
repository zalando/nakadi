package org.zalando.nakadi.service.subscription;

import org.junit.Test;
import org.zalando.nakadi.util.ThreadUtils;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

public class IdleStreamWatcherTest {

    @Test
    public void checkIdleStreamWatcher() throws InterruptedException {
        final IdleStreamWatcher idleStreamWatcher = new IdleStreamWatcher(100);

        assertThat(idleStreamWatcher.isIdleForToolLong(), is(false)); // not idle at all

        idleStreamWatcher.idleStart();
        assertThat(idleStreamWatcher.isIdleForToolLong(), is(false)); // is idle not long enough

        ThreadUtils.sleep(120);
        assertThat(idleStreamWatcher.isIdleForToolLong(), is(true)); // is idle for too long

        idleStreamWatcher.idleEnd();
        assertThat(idleStreamWatcher.isIdleForToolLong(), is(false)); // not idle at all
    }
}
