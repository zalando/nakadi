package org.zalando.nakadi.repository.zookeeper;

import org.apache.curator.framework.CuratorFramework;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;

import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class RotatingCuratorFramework {

    private static final Logger LOG =
            LoggerFactory.getLogger(RotatingCuratorFramework.class);

    private final ZooKeeperHolder zooKeeperHolder;
    private final long curatorMaxLifetimeMs;
    private final ReadWriteLock lock;

    private final CuratorUsage activeCurator;
    private final CuratorUsage retiringCurator;

    private long activeCuratorRotatedAt;

    public RotatingCuratorFramework(
            final ZooKeeperHolder zooKeeperHolder,
            @Value("${nakadi.rotating.curator.max.lifetime.ms:300000}") final long curatorMaxLifetimeMs) {
        this.zooKeeperHolder = zooKeeperHolder;
        this.curatorMaxLifetimeMs = curatorMaxLifetimeMs;
        this.lock = new ReentrantReadWriteLock();
        this.activeCurator = new CuratorUsage
                (zooKeeperHolder.newCuratorFramework(),
                        new AtomicLong());
        this.retiringCurator = new CuratorUsage(null, null);
        this.activeCuratorRotatedAt = System.currentTimeMillis();
    }

    public CuratorFramework takeCuratorFramework() {
        lock.readLock().lock();
        try {
            activeCurator.counter.incrementAndGet();
            return activeCurator.curator;
        } finally {
            lock.readLock().unlock();
        }
    }

    public void returnCuratorFramework(
            final CuratorFramework curatorFramework)
            throws IllegalStateException,
            IllegalArgumentException {

        if (curatorFramework == null) {
            throw new IllegalArgumentException(
                    "CuratorFramework can not be null");
        }

        lock.readLock().lock();
        try {
            if (curatorFramework == activeCurator.curator) {
                activeCurator.counter.decrementAndGet();
            } else if (curatorFramework == retiringCurator.curator) {
                retiringCurator.counter.decrementAndGet();
            } else {
                throw new IllegalStateException(
                        "unexpected returned curator framework");
            }
        } finally {
            lock.readLock().unlock();
        }
    }

    @Scheduled(fixedRateString =
            "${nakadi.rotating.curator.rotation.check.ms:10000}")
    public void scheduleRotationCheck() {
        try {
            if (System.currentTimeMillis() >
                    activeCuratorRotatedAt + curatorMaxLifetimeMs) {
                checkAndRotateCurator();
            }

            checkAndRecycleCurator();
        } catch (final RuntimeException re) {
            LOG.error("failed to run rotation task, will try again", re);
        }
    }

    private void checkAndRotateCurator() {
        if (retiringCurator.curator == null) {
            final CuratorFramework tmpCurator = activeCurator.curator;
            final AtomicLong tmpCounter = activeCurator.counter;
            lock.writeLock().lock();
            try {
                activeCurator.curator = zooKeeperHolder
                        .newCuratorFramework();
                activeCurator.counter = new AtomicLong();
                retiringCurator.curator = tmpCurator;
                retiringCurator.counter = tmpCounter;
            } finally {
                lock.writeLock().unlock();
            }
            activeCuratorRotatedAt = System.currentTimeMillis();
            LOG.info("curator client rotated, current usage {}",
                    retiringCurator.counter);
        } else {
            LOG.debug("waiting for retired client recycling, " +
                    "current usage {}", retiringCurator.counter);
        }
    }

    private void checkAndRecycleCurator() {
        if (retiringCurator.counter != null &&
                retiringCurator.counter.get() == 0) {
            lock.writeLock().lock();
            try {
                retiringCurator.curator.close();
                retiringCurator.curator = null;
                retiringCurator.counter = null;
                LOG.info("curator client recycled");
            } finally {
                lock.writeLock().unlock();
            }
        }
    }

    private class CuratorUsage {
        private CuratorFramework curator;
        private AtomicLong counter;

        private CuratorUsage(
                final CuratorFramework curator,
                final AtomicLong counter) {
            this.curator = curator;
            this.counter = counter;
        }
    }
}
