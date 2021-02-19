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

    private CuratorFramework curatorToRelease;
    private CuratorFramework curatorLastUsed;
    private InterProcessSemaphoreMutex semaphore;

    public static String getSubscriptionLockPath(final String subscriptionId) {
        return "/nakadi/locks/subscription_" + subscriptionId;
    }

    public CuratorNakadiLock(
            final RotatingCuratorFramework rotatingCuratorFramework,
            final String subscriptionId) {
        this.rotatingCuratorFramework = rotatingCuratorFramework;
        this.lockPath = getSubscriptionLockPath(subscriptionId);
    }

    @Override
    public boolean lock() {
        try {
            curatorToRelease = rotatingCuratorFramework
                    .takeCuratorFramework();
            // the check is to not recreate semaphore every time
            if (curatorToRelease != curatorLastUsed) {
                curatorLastUsed = curatorToRelease;
                semaphore = new InterProcessSemaphoreMutex(
                        curatorLastUsed, lockPath);
            }

            final boolean acquired = semaphore.acquire(
                    SECONDS_TO_WAIT_FOR_LOCK,
                    TimeUnit.SECONDS);
            if (!acquired) {
                LOG.warn("failed to acquire semaphore mutex within {} seconds",
                        SECONDS_TO_WAIT_FOR_LOCK);
                tryReturnCuratorFamework();
                return false;
            }

            return true;
        } catch (final Exception e) {
            LOG.error("error while acquiring semaphore mutex lock", e);
            tryReturnCuratorFamework();
            return false;
        }
    }

    private void tryReturnCuratorFamework() {
        if (curatorToRelease != null) {
            try {
                rotatingCuratorFramework.returnCuratorFramework(
                        curatorToRelease);
                curatorToRelease = null;
            } catch (final RuntimeException re) {
                LOG.error("error while returning curator framework", re);
            }
        }
    }

    @Override
    public void unlock() {
        try {
            if (semaphore == null) {
                return;
            }
            semaphore.release();
        } catch (final Exception e) {
            LOG.error("error while releasing curator lock", e);
        } finally {
            tryReturnCuratorFamework();
        }
    }
}
