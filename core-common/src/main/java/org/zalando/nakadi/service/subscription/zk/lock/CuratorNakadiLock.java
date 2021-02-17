package org.zalando.nakadi.service.subscription.zk.lock;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.locks.InterProcessSemaphoreMutex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zalando.nakadi.repository.zookeeper.RotatingCuratorFramework;

import java.util.concurrent.TimeUnit;

/**
 * Not threads safe, current usage pattern does not require it
 */
public class CuratorNakadiLock implements NakadiLock {

    private static final Logger LOG =
            LoggerFactory.getLogger(CuratorNakadiLock.class);
    private static final int SECONDS_TO_WAIT_FOR_LOCK = 15;

    private final RotatingCuratorFramework rotatingCuratorFramework;
    private final String lockPath;

    private CuratorFramework currentCuratorFramework;
    private InterProcessSemaphoreMutex lock;

    public static String getSubscriptionLockPath(final String subscriptionId) {
        return "/nakadi/locks/subscription_" + subscriptionId;
    }

    public CuratorNakadiLock(
            final RotatingCuratorFramework rotatingCuratorFramework,
            final String subscriptionId) {
        this.rotatingCuratorFramework = rotatingCuratorFramework;
        this.lockPath = getSubscriptionLockPath(subscriptionId);
        this.currentCuratorFramework = null;
    }

    @Override
    public boolean lock() {
        try {
            final CuratorFramework curatorFramework =
                    rotatingCuratorFramework.takeCuratorFramework();
            if (currentCuratorFramework != curatorFramework) {
                lock = new InterProcessSemaphoreMutex(
                        curatorFramework, lockPath);
                currentCuratorFramework = curatorFramework;
            }

            final boolean acquired = lock.acquire(
                    SECONDS_TO_WAIT_FOR_LOCK,
                    TimeUnit.SECONDS);
            if (!acquired) {
                LOG.warn("failed to acquire semaphore mutex within {} seconds",
                        SECONDS_TO_WAIT_FOR_LOCK);
            }
            return acquired;
        } catch (final Exception e) {
            LOG.error("error while acquiring semaphore mutex lock", e);
            return false;
        }
    }

    @Override
    public void unlock() {
        try {
            lock.release();
            rotatingCuratorFramework.returnCuratorFramework(
                    currentCuratorFramework);
        } catch (final Exception e) {
            LOG.error("error while releasing curator lock", e);
        }
    }
}
